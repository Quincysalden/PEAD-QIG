"""
LSEG Earnings Dates (Quarterly) with Fiscal Period Labels
=============================================================
Output columns (CSV):
  - Ticker
  - EarningsDateTime (exact timestamp, includes date+time)
  - FiscalPeriod     (e.g., FY2026 Q1)

Terminal output: short summary only.
"""

import lseg.data as ld
import pandas as pd
import time
from datetime import date


# -----------------------------
# CONFIG 
# -----------------------------
CHUNK_SIZE = 25
SLEEP_S = 0.15
OUTPUT_CSV = "quarterly_earnings_dates.csv"

QUARTERS_BACK = 110
TICKERS = [
    "GTE.A", "MI.A", "SII.TO", "A.N", "AA.N", "AAL.O", "AAP.N", "AAPL.O", "ABBV.N", "ABNB.O", "ABT.N", "ABX.N", "ACGL.O",
    "ACN.N", "ADBE.O", "ADCT.N", "ADI.N", "ADM.N", "ADNT.N", "ADP.O", "ADPT.O", "ADSK.O", "ADT.N",
    "AEE.N", "AEP.O", "AES.N", "AFL.N", "AIG.N", "AIRC.N", "AIV.N", "AIZ.N", "AJG.N", "AKAM.O",
    "AL.N", "ALB.N", "ALGN.O", "ALK.N", "ALL.N", "ALLE.N", "AM.N", "AMAT.O", "AMCR.N", "AMD.O",
    "AME.N", "AMG.N", "AMGN.O", "AMP.N", "AMR.N", "AMT.N", "AMTM.N", "AMZN.O", "AN.N", "ANET.N",
    "ANF.N", "ANSS.O", "AON.N", "AOS.N", "APA.O", "APD.N", "APH.N", "APO.N", "APTV.N", "ARC.N",
    "ARE.N", "ASH.N", "ASIX.N", "ASO.N", "ATI.N", "ATO.N", "AVB.N", "AVGO.O", "AVY.N", "AWK.N",
    "AXON.O", "AXP.N", "AYI.N", "AZO.N", "BA.N", "BAC.N", "BALL.N", "BAX.N", "BBBY.N", "BBWI.N",
    "BBY.N", "BC.N", "BDX.N", "BEAM.O", "BEN.N", "BG.N", "BHF.N", "BIG.N", "BIIB.O", "BIO.N",
    "BIVV.O", "BK.N", "BKNG.O", "BKR.N", "BLDR.N", "BLK.N", "BMY.N", "BR.N", "BRO.N", "BSX.N",
    "BTU.N", "BUD.N", "BWA.N", "BX.N", "BXP.N", "C.N", "CAG.N", "CAH.N", "CARR.N", "CAT.N",
    "CB.N", "CBOE.N", "CBRE.N", "CC.N", "CCI.N", "CCK.N", "CCL.N", "CCU.N", "CDNS.O", "CDW.N",
    "CE.N", "CEG.N", "CF.N", "CFG.N", "CG.N", "CHA.N", "CHD.N", "CHK.N", "CHRW.O", "CHTR.O",
    "CI.N", "CIEN.N", "CINF.O", "CL.N", "CLF.N", "CLX.N", "CMA.N", "CMCSA.O", "CME.N", "CMG.N",
    "CMI.N", "CMS.N", "CNC.N", "CNP.N", "CNX.N", "COF.N", "COO.N", "COP.N", "COR.N", "COST.O",
    "COTY.N", "CPAY.N", "CPB.O", "CPRI.N", "CPRT.O", "CPT.N", "CR.N", "CRL.N", "CRM.N", "CRWD.O",
    "CSCO.O", "CSGP.O", "CSR.N", "CSX.O", "CTAS.O", "CTLT.N", "CTRA.N", "CTSH.O", "CTVA.N", "CVS.N",
    "CVX.N", "CZR.N", "D.N", "DAL.N", "DAY.N", "DD.N", "DDS.N", "DE.N", "DECK.N", "DELL.N",
    "DG.N", "DGX.N", "DHI.N", "DHR.N", "DIS.N", "DLR.N", "DLTR.O", "DLX.N", "DNB.N", "DO.N",
    "DOC.N", "DOV.N", "DOW.N", "DPZ.O", "DRI.N", "DTE.N", "DUK.N", "DV.N", "DVA.N", "DVN.N",
    "DXC.N", "DXCM.O", "DYN.N", "EA.O", "EBAY.O", "EC.N", "ECL.N", "ED.N", "EFX.N", "EG.N",
    "EIX.N", "EL.N", "ELV.N", "EMN.N", "EMR.N", "ENPH.O", "EOG.N", "EPAM.N", "EQ.N", "EQIX.O",
    "EQR.N", "EQT.N", "ERIE.O", "ES.N", "ESS.N", "ETN.N", "ETR.N", "ETSY.O", "EVRG.O", "EW.N",
    "EXC.O", "EXPD.N", "EXPE.O", "EXR.N", "F.N", "FANG.O", "FAST.O", "FBIN.N", "FCPT.N", "FCX.N",
    "FDS.N", "FDX.N", "FE.N", "FFIV.O", "FHN.N", "FI.N", "FICO.N", "FIS.N", "FITB.O", "FL.N",
    "FLR.N", "FLS.N", "FMC.N", "FOSL.O", "FOX.O", "FOXA.O", "FRT.N", "FSLR.O", "FSR.N", "FTI.N",
    "FTNT.O", "FTRE.O", "FTV.N", "G.N", "GAP.N", "GD.N", "GDDY.N", "GE.N", "GEHC.O", "GEN.N",
    "GEV.N", "GHC.N", "GIC.N", "GILD.O", "GIS.N", "GL.N", "GLW.N", "GM.N", "GME.N", "GNRC.N",
    "GNW.N", "GOOG.O", "GOOGL.O", "GP.N", "GPC.N", "GPN.N", "GPS.N", "GRMN.N", "GS.N", "GT.N",
    "GTX.N", "GWW.N", "H.N", "HAL.N", "HAS.O", "HBAN.O", "HBI.N", "HCA.N", "HD.N",
    "HES.N", "HI.N", "HIG.N", "HII.N", "HLT.N", "HOG.N", "HOLX.O", "HON.O", "HP.N", "HPE.N",
    "HPQ.N", "HRB.N", "HRL.N", "HSIC.O", "HST.N", "HSY.N", "HUBB.N", "HUM.N", "HWM.N", "IBM.N",
    "ICE.N", "IDXX.O", "IEX.N", "IFF.N", "IGT.N", "ILMN.O", "INCY.O", "INTC.O", "INTU.O", "INVH.N",
    "IP.N", "IPG.N", "IPGP.O", "IQV.N", "IR.N", "IRM.N", "ISRG.O", "IT.N", "ITT.N", "ITW.N",
    "IVZ.N", "J.N", "JBHT.O", "JBL.N", "JCI.N", "JEF.N", "JKHY.O", "JNJ.N", "JNPR.N", "JPM.N",
    "K.N", "KBH.N", "KDP.O", "KEY.N", "KEYS.N", "KG.N", "KHC.O", "KIM.N", "KKR.N", "KLAC.O",
    "KLG.N", "KMB.O", "KMI.N", "KMX.N", "KO.N", "KR.N", "KSS.N", "KTB.N", "KVUE.N", "L.N",
    "LDOS.N", "LEG.N", "LEN.N", "LH.N", "LHX.N", "LIFE.O", "LII.N", "LIN.N", "LKQ.N", "LLY.N",
    "LMT.N", "LNC.N", "LNT.N", "LOW.N", "LPX.N", "LRCX.O", "LU.N", "LULU.O", "LUMN.N", "LUV.N",
    "LVS.N", "LW.N", "LYB.N", "LYV.N", "M.N", "MA.N", "MAA.N", "MAC.N", "MAR.O", "MAS.N",
    "MAT.N", "MBC.N", "MBI.N", "MCD.N", "MCHP.O", "MCK.N", "MCO.N", "MDLZ.O", "MDT.N", "MET.N",
    "META.O", "MGM.N", "MHK.N", "MIR.N", "MKC.N", "MKTX.O", "MLM.N", "MMC.N", "MMI.N",
    "MMM.N", "MNST.O", "MO.N", "MOH.N", "MOS.N", "MPC.N", "MPWR.O", "MRK.N", "MRNA.O", "MRO.N",
    "MS.N", "MSCI.N", "MSFT.O", "MSI.N", "MTB.N", "MTCH.O", "MTD.N", "MTG.N", "MTW.N", "MU.O",
    "MUR.N", "NAVI.O", "NBR.N", "NC.N", "NCLH.N", "NDAQ.O", "NDSN.O", "NE.N", "NEE.N", "NEM.N",
    "NFLX.O", "NGVT.N", "NI.N", "NKE.N", "NKTR.O", "NOC.N", "NOV.N", "NOW.N", "NRG.N", "NSC.N",
    "NTAP.O", "NTRS.O", "NUE.N", "NVDA.O", "NVR.N", "NVT.N", "NWL.N", "NWS.O", "NWSA.O", "NXPI.O",
    "NYT.N", "O.N", "ODFL.O", "ODP.N", "OGN.N", "OI.N", "OKE.N", "OMC.N", "ON.N", "ORCL.N",
    "ORLY.O", "OTIS.N", "OXY.N", "PANW.O", "PARA.O", "PAYC.N", "PAYX.O", "PBI.N", "PCAR.O",
    "PCG.N", "PCH.N", "PD.N", "PDCO.O", "PEG.N", "PENN.O", "PEP.O", "PFE.N", "PFG.N", "PG.N",
    "PGR.N", "PH.N", "PHIN.N", "PHM.N", "PKG.N", "PLD.N", "PLL.N", "PLTR.O", "PM.N", "PNC.N",
    "PNR.N", "PNW.N", "PODD.O", "POOL.O", "PPG.N", "PPL.N", "PRGO.N", "PRU.N", "PSA.N", "PSX.N",
    "PTC.N", "PVH.N", "PWR.N", "PX.N", "PXD.N", "PYPL.O", "Q.N", "QCOM.O", "QRVO.O", "R.N",
    "RAL.N", "RCL.N", "REG.N", "REGN.O", "REZI.N", "RF.N", "RHI.N", "RIG.N", "RJF.N", "RL.N",
    "RMD.N", "ROK.N", "ROL.N", "ROP.N", "ROST.O", "RRC.N", "RRD.N", "RSG.N", "RTX.N", "RVTY.N",
    "S.N", "SANM.O", "SBAC.O", "SBUX.O", "SCHW.N", "SE.N", "SEDG.O", "SEE.N", "SEG.N", "SGI.N",
    "SHW.N", "SIG.N", "SJM.N", "SLB.N", "SLE.N", "SLG.N", "SLM.N", "SMCI.O", "SNA.N",
    "SNDK.O", "SNPS.O", "SNV.N", "SO.N", "SOLV.N", "SPG.N", "SPGI.N", "SRCL.O", "SRE.N", "SSP.N",
    "STE.N", "STLD.O", "STR.N", "STT.N", "STX.O", "STZ.N", "SUN.N", "SW.N", "SWK.N", "SWKS.O",
    "SWN.N", "SYF.N", "SYK.N", "SYY.N", "T.N", "TAP.N", "TDC.N", "TDG.N", "TDY.N", "TE.N",
    "TECH.O", "TEL.N", "TER.N", "TEX.N", "TFC.N", "TFX.N", "TGNA.N", "TGT.N", "THC.N", "TJX.N",
    "TKR.N", "TMC.N", "TMO.N", "TMUS.O", "TPL.N", "TPR.N", "TRGP.N", "TRIP.O", "TRMB.O", "TROW.O",
    "TRV.N", "TSCO.O", "TSLA.O", "TSN.N", "TT.N", "TTWO.O", "TUP.N", "TX.N", "TXN.O", "TXT.N",
    "TYL.N", "U.N", "UA.N", "UAA.N", "UAL.N", "UBER.N", "UCL.N", "UDR.N", "UHS.N", "UIS.N",
    "ULTA.O", "UNH.N", "UNM.N", "UNP.N", "UPS.N", "URBN.O", "URI.N", "USB.N", "V.N", "VC.N",
    "VFC.N", "VICI.N", "VLO.N", "VLTO.N", "VMC.N", "VNO.N", "VNT.N", "VRSK.O", "VRSN.O", "VRTS.N",
    "VRTX.O", "VST.N", "VTR.N", "VTRS.O", "VZ.N", "WAB.N", "WAT.N", "WB.N", "WBA.O", "WBD.N",
    "WDAY.O", "WDC.N", "WEC.N", "WELL.N", "WEN.N", "WFC.N", "WHR.N", "WM.N", "WMB.N", "WMT.N",
    "WOR.N", "WRB.N", "WRK.N", "WST.N", "WTW.N", "WU.N", "WY.N", "WYNN.O", "X.N", "XEL.O",
    "XOM.N", "XRAY.O", "XRX.N", "XYL.N", "YUM.N", "YUMC.N", "ZBH.N", "ZBRA.O", "ZION.O", "ZTS.N"
]


""" MISSING TICKERS:
MISSING_TICKERS = [
    "ABK", "ABMD", "ABS", "ACAS", "ACE", "ACK", "ACS", "ACV", "ADS", "AET", "AFS", "AGC",
    "AGN", "AKS", "ALTR", "ALXN", "AMCC", "ANDV", "ANDW", "ANR", "APC", "APCC", "APOL", "APY",
    "ARG", "ASN", "AT", "ATVI", "AV", "AVP", "AW", "AWE", "AYE", "AZA", "BCR", "BDK", "BF", "BFO",
    "BGEN", "BGG", "BJS", "BLS", "BMC", "BMET", "BMS", "BNI", "BOL", "BRCM", "BRK", "BRL", "BS",
    "BSC", "BVSN", "BXLT", "CA", "CAM", "CBE", "CBH", "CBS", "CBSS", "CCE", "CD", "CELG", "CEN",
    "CEPH", "CERN", "CFC", "CFN", "CGP", "CHIR", "CIN", "CIT", "CMCSK", "CMVT", "CMX", "CNG",
    "CNXT", "COC", "COL", "COMS", "COV", "CPGX", "CPN", "CPQ", "CPWR", "CS", "CSC", "CSRA", "CTB",
    "CTX", "CTXS", "CVC", "CVET", "CVG", "CVH", "CXO", "DCN", "DDR", "DF", "DFS", "DISCA", "DISCK",
    "DISH", "DJ", "DNR", "DPH", "DPS", "DRE", "DTV", "DVMT", "EDS", "EFU", "EK", "EMC", "ENDP",
    "ENE", "EOP", "EP", "ESRX", "ESV", "ETFC", "EVHC", "FBF", "FDC", "FDO", "FII", "FJ", "FLE",
    "FLIR", "FNM", "FPC", "FRC", "FRE", "FRX", "FSH", "FSL", "FTR", "FWC", "GAS", "GDT", "GDW",
    "GENZ", "GGP", "GLK", "GMCR", "GPU", "GR", "GRA", "GTW", "GX", "HAR", "HCBK", "HCR", "HET",
    "HFC", "HM", "HMA", "HNZ", "HOT", "HPC", "HRC", "HSP", "IACI", "IILG", "IKN", "IMNX", "INFO",
    "JAVA", "JCP", "JDSU", "JHF", "JNS", "JNY", "JOS", "JOY", "JP", "JWN", "KM", "KMG", "KRB",
    "KRFT", "KRI", "KSE", "KSU", "LDG", "LEH", "LIZ", "LLL", "LLTC", "LM", "LO", "LSI", "LVLT",
    "LXK", "MAY", "MDP", "MDR", "MEA", "MEDI", "MEE", "MEL", "MER", "MERQ", "MFE", "MHS", "MIL",
    "MJN", "MKG", "MNK", "MOLX", "MON", "MWW", "MXIM", "MYG", "MYL", "MZ", "N", "NAV", "NBL",
    "NCC", "NCE", "NCR", "NFB", "NFX", "NGH", "NLSN", "NMK", "NOVL", "NSI", "NSM", "NT", "NVLS",
    "NXTL", "NYX", "OAT", "OK", "OMX", "ONE", "OWC", "PALM", "PBCT", "PBG", "PBY", "PCL", "PCP",
    "PCS", "PDG", "PETM", "PGL", "PGN", "PHA", "PMCS", "PMTC", "PNU", "POM", "PRD", "PSFT", "PTV",
    "PVN", "PWER", "PWJ", "QCP", "QEP", "QLGC", "QTRN", "RAD", "RAI", "RATL", "RBK", "RD", "RDC",
    "RHT", "RLM", "RML", "ROH", "RSH", "RTN", "RX", "SAF", "SAI", "SAPE", "SBL", "SBNY", "SCG",
    "SDS", "SEBL", "SFA", "SGP", "SHLD", "SIAL", "SIVB", "SLR", "SMI", "SMS", "SNI", "SOTR",
    "SOV", "SPLS", "SRV", "STI", "STJ", "SUB", "SVU", "SWY", "TEG", "TEK", "TFCF", "TFCFA", "TIE",
    "TIF", "TIN", "TLAB", "TNB", "TOS", "TOY", "TRB", "TRW", "TSG", "TSS", "TWC", "TWTR", "TWX",
    "TXU", "TYC", "UCM", "UK", "UMG", "UN", "UPC", "UPR", "UST", "USW", "UVN", "VAR", "VIAB",
    "VO", "VSM", "VTSS", "WCG", "WCOM", "WFM", "WFR", "WFT", "WIN", "WLA", "WLL", "WLP", "WPX",
    "WWY", "WYE", "WYN", "XEC", "XL", "XLNX", "XTO", "YHOO", "YNR"
]


"""



# -----------------------------
# HELPERS
# -----------------------------
def _chunk_list(xs, chunk_size):
    for i in range(0, len(xs), chunk_size):
        yield xs[i:i + chunk_size]


def _standardize_fperiod(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip()

    import re
    m = re.search(r"FY\s*(\d{4})\s*Q\s*([1-4])", s, flags=re.IGNORECASE)
    if m:
        return f"FY{m.group(1)} Q{m.group(2)}"

    m = re.search(r"FY\s*(\d{4})\s*([1-4])", s, flags=re.IGNORECASE)
    if m:
        return f"FY{m.group(1)} Q{m.group(2)}"

    m = re.search(r"(\d{4})\s*Q\s*([1-4])", s, flags=re.IGNORECASE)
    if m:
        return f"FY{m.group(1)} Q{m.group(2)}"

    m = re.search(r"FQ\s*([1-4])\s*(\d{4})", s, flags=re.IGNORECASE)
    if m:
        return f"FY{m.group(2)} Q{m.group(1)}"

    return s


def _extract_fy(fp: str):
    if not fp:
        return None
    import re
    m = re.search(r"FY(\d{4})\s*Q[1-4]$", fp, flags=re.IGNORECASE)
    return int(m.group(1)) if m else None


def fetch_quarterly_earnings_dates(
    tickers,
    quarters_back=80,
    chunk_size=25,
    sleep_s=0.15,
    drop_future=True,
):
    """
    Returns DataFrame with columns (final):
      Ticker, EarningsDateTime, FiscalPeriod
    """
    if isinstance(tickers, str):
        tickers = [tickers]
    tickers = [t.strip() for t in tickers if isinstance(t, str) and t.strip()]
    if not tickers:
        return pd.DataFrame(columns=["Ticker", "EarningsDateTime", "FiscalPeriod"])

    eps_expr = f"TR.EPSActValue(SDate=0,EDate=-{quarters_back-1},Period=FQ0,Frq=FQ)"
    fields = [
        f"{eps_expr}.Date",
        f"{eps_expr}.fperiod",
    ]

    parts = []
    for tick_chunk in _chunk_list(tickers, chunk_size):
        try:
            df = ld.get_data(universe=tick_chunk, fields=fields)
            if df is not None and not df.empty:
                df = df.copy()
                df.columns = ["Ticker", "EarningsDateTime", "FiscalPeriod"]
                parts.append(df)
        except Exception:
            pass
        time.sleep(sleep_s)

    if not parts:
        return pd.DataFrame(columns=["Ticker", "EarningsDateTime", "FiscalPeriod"])

    out = pd.concat(parts, ignore_index=True)

    # Clean
    out["EarningsDateTime"] = pd.to_datetime(out["EarningsDateTime"], errors="coerce")
    out["FiscalPeriod"] = out["FiscalPeriod"].apply(_standardize_fperiod)
    out.dropna(subset=["Ticker", "EarningsDateTime", "FiscalPeriod"], inplace=True)

    # Keep only FY#### Q# labels
    out = out[out["FiscalPeriod"].str.match(r"^FY\d{4}\sQ[1-4]$", na=False)]

    # Drop future (use date component internally)
    if drop_future:
        out = out[out["EarningsDateTime"].dt.date <= date.today()]


    # De-dupe to one row per (Ticker, FiscalPeriod)
    out.sort_values(["Ticker", "FiscalPeriod", "EarningsDateTime"], ascending=[True, True, False], inplace=True)
    out.drop_duplicates(subset=["Ticker", "FiscalPeriod"], keep="first", inplace=True)

    # Final sort: newest first per ticker
    out.sort_values(["Ticker", "EarningsDateTime"], ascending=[True, False], inplace=True)
    out.reset_index(drop=True, inplace=True)

    return out


# -----------------------------
# RUN SCRIPT
# -----------------------------
def main():
    tickers = [t.strip() for t in TICKERS if isinstance(t, str) and t.strip()]
    tickers_requested = len(tickers)

    ld.open_session()

    df = fetch_quarterly_earnings_dates(
        tickers=tickers,
        quarters_back=QUARTERS_BACK,
        chunk_size=CHUNK_SIZE,
        sleep_s=SLEEP_S,
        drop_future=True,
    )

    df.to_csv(OUTPUT_CSV, index=False)

    # Short terminal summary only
    tickers_returned = df["Ticker"].nunique() if not df.empty else 0
    rows = len(df)

    if rows > 0:
        dmin = df["EarningsDateTime"].dt.date.min()
        dmax = df["EarningsDateTime"].dt.date.max()
        coverage = f"{dmin} \u2192 {dmax}"
    else:
        coverage = "n/a"

    print(f"- Tickers requested: {tickers_requested}")
    print(f"- Tickers returned:  {tickers_returned}")
    print(f"- Rows (events):     {rows}")
    print(f"- Date coverage:     {coverage}")
    print(f"- CSV saved as:      {OUTPUT_CSV}")


if __name__ == "__main__":
    main()