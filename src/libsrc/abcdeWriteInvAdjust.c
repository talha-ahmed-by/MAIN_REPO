static const char *rcsid = "$Id$";

/*#START***********************************************************************
 *  Copyright (c) 2002 RedPrairie Corporation. All rights reserved.
 *
 *#END************************************************************************/


#include <moca_app.h>

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <dcscolwid.h>
#include <dcsgendef.h>
#include <dcserr.h>
#include "intlib.h"

LIBEXPORT 
RETURN_STRUCT *intWriteInvAdjust(char *sesnum_i,
                                 char *srcare_i,
                                 char *dstare_i,
                                 char *srclod_i,
                                 char *srcsub_i,
                                 char *srcdtl_i,
                                 long *untqty_i,
                                 double *catch_qty_i,
                                 char *adj_ref1_i,
                                 char *adj_ref2_i,
                                 char *reacod_i,
                                 char *wh_id_i,
                                 moca_bool_t *hld_flg_i,
                                 char *rttn_id_i,
                                 char *cstms_cnsgnmnt_id_i,
                                 long *cstms_bond_flg_i,
                                 long *dty_stmp_flg_i,
                                 long *attr_chg_flg_i,
                                 long *untcas_i)
{
    RETURN_STRUCT  *CurPtr = NULL;
    mocaDataRes *AreaRes = NULL;
    mocaDataRow *AreaRow;
    long ret_status;
    mocaDataRes *res, *res2;
    mocaDataRow *row, *row2;

    char buffer[8000];

    char srcare[ARECOD_LEN + 1];
    char dstare[ARECOD_LEN + 1];
    char srclod[LODNUM_LEN + 1];
    char srcsub[SUBNUM_LEN + 1];
    char srcdtl[DTLNUM_LEN + 1];
    char *hstare;
    char hstacc[ACCNUM_LEN + 1];
    char hstaccClause[utf8Size(500)];
    char *srcloc;
    char Datatype;
    char sesnum[SESNUM_LEN + 1];
    char *adjare;
    long untqty = 0;
    long tempqty = 0;
    long untcas = 0;
    /* Initialize cstms_bond_flg and dty_stmp_flg to avoid passing
     * unexpected values into the database.
     */
    long cstms_bond_flg = 0;
    long dty_stmp_flg = 0;
    long attr_chg_flg = 0;
    double catch_qty = 0.0;
    double adjcst = 0.0;
    double auto_play_cst_thr = 0.0;
    long src_adjust, dst_adjust;
    char adj_ref1[ADJ_REF1_LEN + 1];
    char adj_ref2[ADJ_REF2_LEN + 1];
    char reacod[REACOD_LEN + 1];
    char invadj_id[INVADJ_ID_LEN + 1];
    char wh_id[WH_ID_LEN + 1];
    char supnum[SUPNUM_LEN + 1];
    char rttn_id[RTTN_ID_LEN + 1];
    char cstms_cnsgnmnt_id[CSTMS_CNSGNMNT_ID_LEN + 1];
    moca_bool_t  cnsg_flg = BOOLEAN_NOTSET;
    moca_bool_t  hld_flg = BOOLEAN_NOTSET;
    long *inv_attr_int1 = NULL;
    long *inv_attr_int2 = NULL;
    long *inv_attr_int3 = NULL;
    long *inv_attr_int4 = NULL;
    long *inv_attr_int5 = NULL;
    double *inv_attr_flt1 = NULL;
    double *inv_attr_flt2 = NULL;
    double *inv_attr_flt3 = NULL;

    memset(srcare, 0, sizeof(srcare));
    memset(dstare, 0, sizeof(dstare));
    memset(srclod, 0, sizeof(srclod));
    memset(srcsub, 0, sizeof(srcsub));
    memset(srcdtl, 0, sizeof(srcdtl));
    memset(sesnum, 0, sizeof(sesnum));
    memset(adj_ref1, 0, sizeof(adj_ref1));
    memset(adj_ref2, 0, sizeof(adj_ref2));
    memset(reacod, 0, sizeof(reacod));
    memset(wh_id, 0, sizeof(wh_id));
    memset(supnum, 0, sizeof(supnum));
    memset(rttn_id, 0, sizeof(rttn_id));
    memset(cstms_cnsgnmnt_id, 0, sizeof(cstms_cnsgnmnt_id));

    if (srcare_i && misTrimLen (srcare_i, ARECOD_LEN))
        misTrimcpy (srcare, srcare_i, ARECOD_LEN);
    else
        return (APPMissingArg ("srcare"));

    if (dstare_i && misTrimLen (dstare_i, ARECOD_LEN))
        misTrimcpy (dstare, dstare_i, ARECOD_LEN);
    else
        return (APPMissingArg ("dstare"));

    if (adj_ref1_i && misTrimLen(adj_ref1_i, ADJ_REF1_LEN))
       misTrimcpy(adj_ref1, adj_ref1_i, ADJ_REF1_LEN);

    if (adj_ref2_i && misTrimLen(adj_ref2_i, ADJ_REF2_LEN))
        misTrimcpy(adj_ref2, adj_ref2_i, ADJ_REF2_LEN);

    if (reacod_i && misTrimLen(reacod_i, REACOD_LEN))
        misTrimcpy(reacod, reacod_i, REACOD_LEN);

    if (misTrimIsNull(wh_id_i, WH_ID_LEN))
        return (APPMissingArg("wh_id"));

    misTrimcpy(wh_id, wh_id_i, WH_ID_LEN);

    if (rttn_id_i && misTrimLen (rttn_id_i, RTTN_ID_LEN))
    {
        misTrimcpy(rttn_id, rttn_id_i, RTTN_ID_LEN);
    }

    if (cstms_cnsgnmnt_id_i &&
        misTrimLen (cstms_cnsgnmnt_id_i, CSTMS_CNSGNMNT_ID_LEN))
    {
        misTrimcpy(cstms_cnsgnmnt_id, cstms_cnsgnmnt_id_i,
                   CSTMS_CNSGNMNT_ID_LEN);
    }

    if (hld_flg_i)
    {
        if (*hld_flg_i == BOOLEAN_TRUE)
            hld_flg = BOOLEAN_TRUE;
        else 
            hld_flg = BOOLEAN_FALSE;
    }

    if (cstms_bond_flg_i)
    {
        cstms_bond_flg = *cstms_bond_flg_i;
    }

    if (dty_stmp_flg_i)
    {
        dty_stmp_flg = *dty_stmp_flg_i;
    }
    
    if (attr_chg_flg_i)
    {
        attr_chg_flg = *attr_chg_flg_i;
    }   

    /* If we haven't read the areas, then get them now */
    osSnprintf(buffer, sizeof(buffer),
            " select distinct aremst.arecod, "
            "       loc_typ.adjflg, "
            "       loc_typ.expflg, "
            "       loc_typ.shpflg "
            "  from aremst "
            "  join locmst "
            "       on aremst.arecod = locmst.arecod "
            "       and aremst.wh_id = locmst.wh_id "
            "  join loc_typ "
            "       on loc_typ.loc_typ_id = locmst.loc_typ_id "                
            " where aremst.wh_id = '%s' "
            "   and aremst.arecod in ('%s','%s') "
            "   and (loc_typ.expflg = %ld "
            "    or loc_typ.shpflg = %ld "
            "    or (loc_typ.adjflg = %ld and loc_typ.fwiflg = %ld))",
            wh_id,
            srcare,
            dstare,
            BOOLEAN_TRUE, BOOLEAN_TRUE,
            BOOLEAN_TRUE, BOOLEAN_FALSE);
    ret_status = sqlExecStr(buffer, &AreaRes);
    if (ret_status != eOK &&
        ret_status != eDB_NO_ROWS_AFFECTED)
    {
        srvFreeMemory(SRVRET_STRUCT, AreaRes);
        return (srvResults(ret_status, NULL));
    }

    src_adjust = dst_adjust = 0;
    for (AreaRow = sqlGetRow(AreaRes); AreaRow;
         AreaRow = sqlGetNextRow(AreaRow))
    {
        if ((misStrncmpChars(misTrim(srcare), sqlGetString(AreaRes, AreaRow, "arecod"),
                    ARECOD_LEN) == 0) &&
            (sqlGetBoolean(AreaRes, AreaRow, "adjflg") == BOOLEAN_TRUE))
            break;
    }

    if (AreaRow != NULL)
        src_adjust = 1;

    for (AreaRow = sqlGetRow(AreaRes); AreaRow; 
         AreaRow = sqlGetNextRow(AreaRow))
    {
        if ((misStrncmpChars(misTrim(dstare),sqlGetString(AreaRes, AreaRow, "arecod"),
                    ARECOD_LEN) == 0) &&
            (sqlGetBoolean(AreaRes, AreaRow, "adjflg") == BOOLEAN_TRUE))
            break;
    }
    if (AreaRow != NULL)
        dst_adjust = 1;

    if ((dst_adjust && src_adjust) || (!dst_adjust && !src_adjust))
        return (srvResults(eOK, NULL));

    for (AreaRow = sqlGetRow(AreaRes); AreaRow; 
         AreaRow = sqlGetNextRow(AreaRow))
    {
        if ((misStrncmpChars(misTrim(dstare), sqlGetString(AreaRes, AreaRow, "arecod"),
                    ARECOD_LEN) == 0) &&
            (sqlGetBoolean(AreaRes, AreaRow, "adjflg") == BOOLEAN_FALSE))
            break;
    }
    if (src_adjust && AreaRow != NULL)
        return (srvResults(eOK, NULL));

    for (AreaRow = sqlGetRow(AreaRes); AreaRow; 
         AreaRow = sqlGetNextRow(AreaRow))
    {
        if ((misStrncmpChars(misTrim(srcare), sqlGetString(AreaRes, AreaRow, "arecod"),
                    ARECOD_LEN) == 0) &&
            (sqlGetBoolean(AreaRes, AreaRow, "adjflg") == BOOLEAN_FALSE))
            break;
    }
    if (dst_adjust && AreaRow != NULL)
        return (srvResults(eOK, NULL));

    if (dst_adjust)
    {
        adjare = dstare;
        hstare = srcare;
    }
    else
    {
        adjare = srcare;
        hstare = dstare;
    }

    if (AreaRes)
        sqlFreeResults(AreaRes);

    if (srclod_i && misTrimLen(srclod_i, LODNUM_LEN))
        misTrimcpy(srclod, srclod_i, LODNUM_LEN);

    if (srcsub_i && misTrimLen(srcsub_i, SUBNUM_LEN))
        misTrimcpy(srcsub, srcsub_i, SUBNUM_LEN);

    if (srcdtl_i && misTrimLen(srcdtl_i, DTLNUM_LEN))
        misTrimcpy(srcdtl, srcdtl_i, DTLNUM_LEN);

    if (sesnum_i && misTrimLen(sesnum_i, SESNUM_LEN))
        misTrimcpy(sesnum, sesnum_i, SESNUM_LEN);
    else
        return (srvResults(eOK, NULL));

    srvGetNeededElement("srcloc", "", &Datatype, (void **) &srcloc);

    if (misTrimLen(srclod, LODNUM_LEN) == 0 &&
        misTrimLen(srcsub, SUBNUM_LEN) == 0 &&
        misTrimLen(srcdtl, DTLNUM_LEN) == 0)
        return (srvResults(eINT_REQ_PARAM_MISSING, NULL));

    if (untqty_i)
    {
        untqty = *untqty_i;
        tempqty = untqty;
    }

    if (untcas_i)
    {
        untcas = *untcas_i;
    }

    if (catch_qty_i)
        catch_qty = *catch_qty_i;

    /*
     *  Let's get the predefined threshold cost value
     *  to find out if we need to automatically play
     *  adjustments. 
     */

    osSnprintf(buffer, sizeof(buffer),
            "select nvl(auto_play_cst_thr, 0) auto_play_cst_thr "
            "  from wh "
            " where wh_id  = '%s'",
            wh_id);

    ret_status = sqlExecStr(buffer, &res);

    if (ret_status != eOK)
    {
        if (res) sqlFreeResults(res);
        return (srvResults(ret_status, NULL));
    }
    else
    {
        row = sqlGetRow(res);
        auto_play_cst_thr = sqlGetFloat(res, row, "auto_play_cst_thr");
        sqlFreeResults(res);
    }

    /*
     * Note that in each of the queris below, we join to the part table
     * because we only want to write and report adjustments where the 
     * prdflg (production inventory) is true and the part supports 
     * adjustments (prtadjflg is true).
     */

    if (misTrimLen(srcdtl, DTLNUM_LEN))
    {
        osSnprintf(buffer, sizeof(buffer),
                "select invdtl.prtnum, invdtl.orgcod, invdtl.revlvl, "
                "       invdtl.supnum, "
                "       invdtl.lotnum, "
                "       invdtl.sup_lotnum, "
                "       invdtl.mandte, "
                "       invdtl.expire_dte, "
                "       invdtl.inv_attr_str1, "
                "       invdtl.inv_attr_str2, "
                "       invdtl.inv_attr_str3, "
                "       invdtl.inv_attr_str4, "
                "       invdtl.inv_attr_str5, "
                "       invdtl.inv_attr_str6, "
                "       invdtl.inv_attr_str7, "
                "       invdtl.inv_attr_str8, "
                "       invdtl.inv_attr_str9, "
                "       invdtl.inv_attr_str10, "
                "       invdtl.inv_attr_str11, "
                "       invdtl.inv_attr_str12, "
                "       invdtl.inv_attr_str13, "
                "       invdtl.inv_attr_str14, "
                "       invdtl.inv_attr_str15, "
                "       invdtl.inv_attr_str16, "
                "       invdtl.inv_attr_str17, "
                "       invdtl.inv_attr_str18, "
                "       invdtl.inv_attr_int1, "
                "       invdtl.inv_attr_int2, "
                "       invdtl.inv_attr_int3, "
                "       invdtl.inv_attr_int4, "
                "       invdtl.inv_attr_int5, "
                "       invdtl.inv_attr_flt1, "
                "       invdtl.inv_attr_flt2, "
                "       invdtl.inv_attr_flt3, "
                "       invdtl.inv_attr_dte1, "
                "       invdtl.inv_attr_dte2, "
                "       invdtl.prt_client_id, "
                "       invdtl.invsts, "
                "       invdtl.cnsg_flg, "
                "       invdtl.hld_flg, "
                "       nvl(prtmst_view.untcst, 0) untcst, "
                "       sum(invdtl.untqty) untqty, "
                "       sum(invdtl.catch_qty) catch_qty, "
                "       invdtl.cstms_cnsgnmnt_id, "
                "       invdtl.rttn_id, "
                "       invdtl.cstms_bond_flg, "
                "       invdtl.dty_stmp_flg "
                "  from prtmst_view, "
                "       invlod, "
                "       invsub, "
                "       invdtl "
                " where invdtl.dtlnum        = '%s'"
                "   and invsub.subnum        = invdtl.subnum "
                "   and invlod.lodnum        = invsub.lodnum "
                "   and prtmst_view.wh_id         = invlod.wh_id "
                "   and prtmst_view.prtnum        = invdtl.prtnum "
                "   and prtmst_view.prt_client_id = invdtl.prt_client_id "
                "   and prtmst_view.prdflg        = '%ld' "
                "   and prtmst_view.prtadjflg     = '%ld' "
                " group by invdtl.prtnum, invdtl.prt_client_id, "
                "          invdtl.orgcod, invdtl.revlvl, "
                "          invdtl.supnum, "
                "          invdtl.lotnum, "
                "          invdtl.sup_lotnum, "
                "          invdtl.mandte, "
                "          invdtl.expire_dte, "
                "          invdtl.inv_attr_str1, "
                "          invdtl.inv_attr_str2, "
                "          invdtl.inv_attr_str3, "
                "          invdtl.inv_attr_str4, "
                "          invdtl.inv_attr_str5, "
                "          invdtl.inv_attr_str6, "
                "          invdtl.inv_attr_str7, "
                "          invdtl.inv_attr_str8, "
                "          invdtl.inv_attr_str9, "
                "          invdtl.inv_attr_str10, "
                "          invdtl.inv_attr_str11, "
                "          invdtl.inv_attr_str12, "
                "          invdtl.inv_attr_str13, "
                "          invdtl.inv_attr_str14, "
                "          invdtl.inv_attr_str15, "
                "          invdtl.inv_attr_str16, "
                "          invdtl.inv_attr_str17, "
                "          invdtl.inv_attr_str18, "
                "          invdtl.inv_attr_int1, "
                "          invdtl.inv_attr_int2, "
                "          invdtl.inv_attr_int3, "
                "          invdtl.inv_attr_int4, "
                "          invdtl.inv_attr_int5, "
                "          invdtl.inv_attr_flt1, "
                "          invdtl.inv_attr_flt2, "
                "          invdtl.inv_attr_flt3, "
                "          invdtl.inv_attr_dte1, "
                "          invdtl.inv_attr_dte2, "
                "          invdtl.prt_client_id, "
                "          invdtl.invsts, "
                "          invdtl.cnsg_flg, "
                "          invdtl.hld_flg, "
                "          prtmst_view.untcst, "
                "          invdtl.cstms_cnsgnmnt_id, "
                "          invdtl.rttn_id, "
                "          invdtl.cstms_bond_flg, "
                "          invdtl.dty_stmp_flg ",
                srcdtl,
                (long)BOOLEAN_TRUE,
                (long)BOOLEAN_TRUE);
    }
    else if (misTrimLen(srcsub, SUBNUM_LEN))
    {
        osSnprintf(buffer, sizeof(buffer),
                "select invdtl.prtnum, invdtl.orgcod, invdtl.revlvl, "
                "       invdtl.supnum, "
                "       invdtl.lotnum, "
                "       invdtl.sup_lotnum, "
                "       invdtl.mandte, "
                "       invdtl.expire_dte, "
                "       invdtl.inv_attr_str1, "
                "       invdtl.inv_attr_str2, "
                "       invdtl.inv_attr_str3, "
                "       invdtl.inv_attr_str4, "
                "       invdtl.inv_attr_str5, "
                "       invdtl.inv_attr_str6, "
                "       invdtl.inv_attr_str7, "
                "       invdtl.inv_attr_str8, "
                "       invdtl.inv_attr_str9, "
                "       invdtl.inv_attr_str10, "
                "       invdtl.inv_attr_str11, "
                "       invdtl.inv_attr_str12, "
                "       invdtl.inv_attr_str13, "
                "       invdtl.inv_attr_str14, "
                "       invdtl.inv_attr_str15, "
                "       invdtl.inv_attr_str16, "
                "       invdtl.inv_attr_str17, "
                "       invdtl.inv_attr_str18, "
                "       invdtl.inv_attr_int1, "
                "       invdtl.inv_attr_int2, "
                "       invdtl.inv_attr_int3, "
                "       invdtl.inv_attr_int4, "
                "       invdtl.inv_attr_int5, "
                "       invdtl.inv_attr_flt1, "
                "       invdtl.inv_attr_flt2, "
                "       invdtl.inv_attr_flt3, "
                "       invdtl.inv_attr_dte1, "
                "       invdtl.inv_attr_dte2, "
                "       invdtl.prt_client_id, "
                "       invdtl.invsts, "
                "       invdtl.cnsg_flg, "
                "       invdtl.hld_flg, "
                "       nvl(prtmst_view.untcst, 0) untcst, "
                "       sum(invdtl.untqty) untqty, "
                "       sum(invdtl.catch_qty) catch_qty, "
                "       invdtl.cstms_cnsgnmnt_id, "
                "       invdtl.rttn_id, "
                "       invdtl.cstms_bond_flg, "
                "       invdtl.dty_stmp_flg "
                "  from prtmst_view, "
                "       invlod, "
                "       invsub, "
                "       invdtl "
                " where invdtl.subnum = '%s' "
                "   and invsub.subnum        = invdtl.subnum "
                "   and invlod.lodnum        = invsub.lodnum "
                "   and prtmst_view.wh_id         = invlod.wh_id "
                "   and prtmst_view.prt_client_id = invdtl.prt_client_id "
                "   and prtmst_view.prtnum        = invdtl.prtnum "
                "   and prtmst_view.prdflg        = '%ld' "
                "   and prtmst_view.prtadjflg     = '%ld' "
                " group by invdtl.prtnum, invdtl.prt_client_id, "
                "          invdtl.orgcod, invdtl.revlvl, "
                "          invdtl.supnum, "
                "          invdtl.lotnum, "
                "          invdtl.sup_lotnum, "
                "          invdtl.mandte, "
                "          invdtl.expire_dte, "
                "          invdtl.inv_attr_str1, "
                "          invdtl.inv_attr_str2, "
                "          invdtl.inv_attr_str3, "
                "          invdtl.inv_attr_str4, "
                "          invdtl.inv_attr_str5, "
                "          invdtl.inv_attr_str6, "
                "          invdtl.inv_attr_str7, "
                "          invdtl.inv_attr_str8, "
                "          invdtl.inv_attr_str9, "
                "          invdtl.inv_attr_str10, "
                "          invdtl.inv_attr_str11, "
                "          invdtl.inv_attr_str12, "
                "          invdtl.inv_attr_str13, "
                "          invdtl.inv_attr_str14, "
                "          invdtl.inv_attr_str15, "
                "          invdtl.inv_attr_str16, "
                "          invdtl.inv_attr_str17, "
                "          invdtl.inv_attr_str18, "
                "          invdtl.inv_attr_int1, "
                "          invdtl.inv_attr_int2, "
                "          invdtl.inv_attr_int3, "
                "          invdtl.inv_attr_int4, "
                "          invdtl.inv_attr_int5, "
                "          invdtl.inv_attr_flt1, "
                "          invdtl.inv_attr_flt2, "
                "          invdtl.inv_attr_flt3, "
                "          invdtl.inv_attr_dte1, "
                "          invdtl.inv_attr_dte2, "
                "          invdtl.invsts, "
                "          invdtl.cnsg_flg, "
                "          invdtl.hld_flg, "
                "          prtmst_view.untcst, "
                "          invdtl.cstms_cnsgnmnt_id, "
                "          invdtl.rttn_id, "
                "          invdtl.cstms_bond_flg, "
                "          invdtl.dty_stmp_flg ",
                srcsub,
                (long)BOOLEAN_TRUE,
                (long)BOOLEAN_TRUE);
    }
    else if (misTrimLen(srclod, LODNUM_LEN))
    {
        osSnprintf(buffer, sizeof(buffer),
                "select invdtl.prtnum, invdtl.orgcod, invdtl.revlvl, "
                "       invdtl.supnum, "
                "       invdtl.lotnum, "
                "       invdtl.sup_lotnum, "
                "       invdtl.mandte, "
                "       invdtl.expire_dte, "
                "       invdtl.inv_attr_str1, "
                "       invdtl.inv_attr_str2, "
                "       invdtl.inv_attr_str3, "
                "       invdtl.inv_attr_str4, "
                "       invdtl.inv_attr_str5, "
                "       invdtl.inv_attr_str6, "
                "       invdtl.inv_attr_str7, "
                "       invdtl.inv_attr_str8, "
                "       invdtl.inv_attr_str9, "
                "       invdtl.inv_attr_str10, "
                "       invdtl.inv_attr_str11, "
                "       invdtl.inv_attr_str12, "
                "       invdtl.inv_attr_str13, "
                "       invdtl.inv_attr_str14, "
                "       invdtl.inv_attr_str15, "
                "       invdtl.inv_attr_str16, "
                "       invdtl.inv_attr_str17, "
                "       invdtl.inv_attr_str18, "
                "       invdtl.inv_attr_int1, "
                "       invdtl.inv_attr_int2, "
                "       invdtl.inv_attr_int3, "
                "       invdtl.inv_attr_int4, "
                "       invdtl.inv_attr_int5, "
                "       invdtl.inv_attr_flt1, "
                "       invdtl.inv_attr_flt2, "
                "       invdtl.inv_attr_flt3, "
                "       invdtl.inv_attr_dte1, "
                "       invdtl.inv_attr_dte2, "
                "       invdtl.prt_client_id, "
                "       invdtl.invsts, "
                "       invdtl.supnum, "
                "       invdtl.cnsg_flg, "
                "       invdtl.hld_flg, "
                "       nvl(prtmst_view.untcst, 0) untcst, "
                "       sum(invdtl.untqty) untqty, "
                "       sum(invdtl.catch_qty) catch_qty, "
                "       invdtl.cstms_cnsgnmnt_id, "
                "       invdtl.rttn_id, "
                "       invdtl.cstms_bond_flg, "
                "       invdtl.dty_stmp_flg, "
                "       %ld untcas"
                "  from prtmst_view, "
                "       invdtl, invsub, invlod"
                " where invdtl.subnum = invsub.subnum "
                "   and invsub.lodnum = '%s'"
                "   and invsub.lodnum = invlod.lodnum"
                "   and prtmst_view.wh_id         = invlod.wh_id "
                "   and prtmst_view.prt_client_id = invdtl.prt_client_id "
                "   and prtmst_view.prtnum        = invdtl.prtnum "
                "   and prtmst_view.prdflg        = '%ld' "
                "   and prtmst_view.prtadjflg     = '%ld' "
                " group by invdtl.prtnum, invdtl.prt_client_id, "
                "          invdtl.orgcod, invdtl.revlvl, "
                "          invdtl.supnum, "
                "          invdtl.lotnum, "
                "          invdtl.sup_lotnum, "
                "          invdtl.mandte, "
                "          invdtl.expire_dte, "
                "          invdtl.inv_attr_str1, "
                "          invdtl.inv_attr_str2, "
                "          invdtl.inv_attr_str3, "
                "          invdtl.inv_attr_str4, "
                "          invdtl.inv_attr_str5, "
                "          invdtl.inv_attr_str6, "
                "          invdtl.inv_attr_str7, "
                "          invdtl.inv_attr_str8, "
                "          invdtl.inv_attr_str9, "
                "          invdtl.inv_attr_str10, "
                "          invdtl.inv_attr_str11, "
                "          invdtl.inv_attr_str12, "
                "          invdtl.inv_attr_str13, "
                "          invdtl.inv_attr_str14, "
                "          invdtl.inv_attr_str15, "
                "          invdtl.inv_attr_str16, "
                "          invdtl.inv_attr_str17, "
                "          invdtl.inv_attr_str18, "
                "          invdtl.inv_attr_int1, "
                "          invdtl.inv_attr_int2, "
                "          invdtl.inv_attr_int3, "
                "          invdtl.inv_attr_int4, "
                "          invdtl.inv_attr_int5, "
                "          invdtl.inv_attr_flt1, "
                "          invdtl.inv_attr_flt2, "
                "          invdtl.inv_attr_flt3, "
                "          invdtl.inv_attr_dte1, "
                "          invdtl.inv_attr_dte2, "
                "          invdtl.cnsg_flg, "
                "          invdtl.hld_flg, "
                "          invdtl.invsts, "
                "          prtmst_view.untcst, "
                "          invdtl.cstms_cnsgnmnt_id, "
                "          invdtl.rttn_id, "
                "          invdtl.cstms_bond_flg, "
                "          invdtl.dty_stmp_flg ",
                untcas,
                srclod,
                (long)BOOLEAN_TRUE,
                (long)BOOLEAN_TRUE);
    }
    ret_status = sqlExecStr(buffer, &res);
    if (ret_status != eOK)
    {
        sqlFreeResults(res);

        /*
         * If we got no rows affected, that's OK.  This could happen
         * if the inventory we are looking at had its prdflg set to false.
         */

        if (ret_status == eDB_NO_ROWS_AFFECTED || 
            ret_status == eSRV_NO_ROWS_AFFECTED)
            return (srvResults(eOK, NULL));
        else
            return (srvResults(ret_status, NULL));
    }

    /* Memory for these is needed at this point to continue. */
    inv_attr_int1 = (long *)calloc(1,sizeof(long));
    inv_attr_int2 = (long *)calloc(1,sizeof(long));
    inv_attr_int3 = (long *)calloc(1,sizeof(long));
    inv_attr_int4 = (long *)calloc(1,sizeof(long));
    inv_attr_int5 = (long *)calloc(1,sizeof(long));
    inv_attr_flt1 = (double *)calloc(1,sizeof(double));
    inv_attr_flt2 = (double *)calloc(1,sizeof(double));
    inv_attr_flt3 = (double *)calloc(1,sizeof(double));

    for (row = sqlGetRow(res); row; row = sqlGetNextRow(row))
    {
        if(!sqlIsNull(res, row, "inv_attr_int1"))
        {
            *inv_attr_int1 = sqlGetLong(res, row, "inv_attr_int1");
        }
        if(!sqlIsNull(res, row, "inv_attr_int2"))
        {
            *inv_attr_int2 = sqlGetLong(res, row, "inv_attr_int2");
        }
        if(!sqlIsNull(res, row, "inv_attr_int3"))
        {
            *inv_attr_int3 = sqlGetLong(res, row, "inv_attr_int3");
        }
        if(!sqlIsNull(res, row, "inv_attr_int4"))
        {
            *inv_attr_int4 = sqlGetLong(res, row, "inv_attr_int4");
        }
        if(!sqlIsNull(res, row, "inv_attr_int5"))
        {
            *inv_attr_int5 = sqlGetLong(res, row, "inv_attr_int5");
        }
        if(!sqlIsNull(res, row, "inv_attr_flt1"))
        {
            *inv_attr_flt1 = sqlGetFloat(res, row, "inv_attr_flt1");
        }
        if(!sqlIsNull(res, row, "inv_attr_flt2"))
        {
            *inv_attr_flt2 = sqlGetFloat(res, row, "inv_attr_flt2");
        }
        if(!sqlIsNull(res, row, "inv_attr_flt3"))
        {
            *inv_attr_flt3 = sqlGetFloat(res, row, "inv_attr_flt3");
        }
        /* TODO: Validate that the client_id should be added to the policy
         * checking.  */
        /* Find out where the host thinks this product is */
        /* This will be a command at some point */

        memset(hstacc, 0, sizeof(hstacc));
        osSnprintf(buffer, sizeof(buffer),
                "select pol2.rtstr1 rtstr1 "
                "  from poldat_view pol1, poldat_view pol2 "
                " where pol1.polcod = 'HOST-TRANS' "
                "   and pol1.polvar = 'AREA-HOST-LEVEL' "
                "   and pol1.polval = '%s' "
                "   and pol2.polcod = 'HOST-TRANS' "
                "   and pol2.polvar = 'AREA-HOST-LOCATION' "
                "   and pol1.polval = pol2.polval "
                "   and pol1.rtstr1 = 'S' "
                "   and pol1.wh_id  = '%s' "
                "   and pol1.wh_id  = pol2.wh_id "
                "   and (pol1.rtstr2 is null or pol1.rtstr2 = '') "
                "union  "
                " select pol2.rtstr1 "
                "   from poldat_view pol1, poldat_view pol2 "
                "  where pol1.polcod = 'HOST-TRANS' "
                "    and pol1.polvar = 'AREA-HOST-LEVEL' "
                "    and pol1.polval = '%s' "
                "    and pol2.polcod = 'HOST-TRANS' "
                "    and pol2.polvar = 'AREA-HOST-LOCATION' "
                "    and upper(pol2.polval) = '%s_INVSTS_%s' "
                "    and pol1.rtstr1 = 'S' "
                "    and rtrim(upper(pol1.rtstr2)) = 'INVSTS'  "
                "    and pol1.wh_id  = pol2.wh_id "
                "    and pol1.wh_id  = '%s' "
                "union  "
                " select pol2.rtstr1 "
                "   from poldat_view pol1, poldat_view pol2 "
                "  where pol1.polcod = 'HOST-TRANS' "
                "    and pol1.polvar = 'AREA-HOST-LEVEL' "
                "    and pol1.polval = '%s' "
                "    and pol2.polcod = 'HOST-TRANS' "
                "    and pol2.polvar = 'AREA-HOST-LOCATION' "
                "    and upper(pol2.polval) = '%s_REVLVL_%s' "
                "    and pol1.rtstr1 = 'S' "
                "    and rtrim(upper(pol1.rtstr2)) = 'REVLVL'  "
                "    and pol1.wh_id  = pol2.wh_id "
                "    and pol1.wh_id  = '%s' "
                "union  "
                " select pol2.rtstr1 "
                "   from poldat_view pol1, poldat_view pol2 "
                "  where pol1.polcod = 'HOST-TRANS' "
                "    and pol1.polvar = 'AREA-HOST-LEVEL' "
                "    and pol1.polval = '%s' "
                "    and pol2.polcod = 'HOST-TRANS' "
                "    and pol2.polvar = 'AREA-HOST-LOCATION' "
                "    and upper(pol2.polval) = '%s_LOTNUM_%s' "
                "    and pol1.rtstr1 = 'S' "
                "    and rtrim(upper(pol1.rtstr2)) = 'LOTNUM'  "
                "    and pol1.wh_id  = pol2.wh_id "
                "    and pol1.wh_id  = '%s' "
                "union  "
                " select pol2.rtstr1 "
                "   from poldat_view pol1, poldat_view pol2 "
                "  where pol1.polcod = 'HOST-TRANS' "
                "    and pol1.polvar = 'AREA-HOST-LEVEL' "
                "    and pol1.polval = '%s' "
                "    and pol2.polcod = 'HOST-TRANS' "
                "    and pol2.polvar = 'AREA-HOST-LOCATION' "
                "    and upper(pol2.polval) = '%s_ORGCOD_%s' "
                "    and pol1.rtstr1 = 'S' "
                "    and rtrim(upper(pol1.rtstr2)) = 'orgcod' "
                "    and pol1.wh_id  = pol2.wh_id "
                "    and pol1.wh_id  = '%s' "
                "union  "
                " select pol2.rtstr1 "
                "   from poldat_view pol1, poldat_view pol2 "
                "  where pol1.polcod = 'HOST-TRANS' "
                "    and pol1.polvar = 'AREA-HOST-LEVEL' "
                "    and pol1.polval = '%s' "
                "    and pol2.polcod = 'HOST-TRANS' "
                "    and pol2.polvar = 'AREA-HOST-LOCATION' "
                "    and upper(pol2.polval) = '%s_CLIENT_ID_%s' "
                "    and pol1.rtstr1 = 'S' "
                "    and rtrim(upper(pol1.rtstr2)) = 'prt_client_id' "
                "    and pol1.wh_id  = pol2.wh_id "
                "    and pol1.wh_id  = '%s' ",
                hstare,
                wh_id,
                hstare, hstare, sqlGetString(res, row, "invsts"),
                wh_id,
                hstare, hstare, sqlGetString(res, row, "revlvl"),
                wh_id,
                hstare, hstare, sqlGetString(res, row, "lotnum"),
                wh_id,
                hstare, hstare, sqlGetString(res, row, "orgcod"),
                wh_id,
                hstare, hstare, sqlGetString(res, row, "prt_client_id"),
                wh_id);

        res2 = NULL;
        ret_status = sqlExecStr(buffer, &res2);
        if (ret_status != eOK)
        {
            sqlFreeResults(res2);
            res2 = NULL;

            /* See if we are detail tracked */
            osSnprintf(buffer, sizeof(buffer),
                    " get cached policy "
                    "    where polcod = 'HOST-TRANS' "
                    "      and polvar = 'AREA-HOST-LEVEL' "
                    "      and polval = '%s' "
                    "      and colnam = 'rtstr1' "
                    "      and colval = 'D' "
                    "      and wh_id  = '%s' "
                    "      and srtseq = '0' ",
                    hstare,
                    wh_id);
            ret_status = srvInitiateCommand(buffer, NULL);

            if (ret_status == eOK)
                misTrimcpy(hstacc, srcloc, ACCNUM_LEN);
        }
        if (res2)
        {
            row2 = sqlGetRow(res2);
            misTrimcpy(hstacc,sqlGetString(res2, row2, "rtstr1"), ACCNUM_LEN);
            sqlFreeResults(res2);
        }

        if (strlen(hstacc) > 0)
            osSnprintf(hstaccClause, sizeof(hstaccClause), " and hstacc = '%s' ", hstacc);
        else
            osSnprintf(hstaccClause, sizeof(hstaccClause), " and hstacc = '%s' ", 
                    sqlGetString(res, row, "invsts"));

        memset(invadj_id, 0, sizeof(invadj_id));
        ret_status = appNextNum(NUMCOD_INVADJ_ID, invadj_id);
        if (ret_status != eOK)
        {
            free(inv_attr_int1);
            free(inv_attr_int2);
            free(inv_attr_int3);
            free(inv_attr_int4);
            free(inv_attr_int5);
            free(inv_attr_flt1);
            free(inv_attr_flt2);
            free(inv_attr_flt3);
            return(srvResults(ret_status, NULL));
        }

        /*
         * As of the 6.1.0 release, we will now be storing a record for
         * each adjustment.  Prior to that, we would summarize on the
         * sesnum, prtnum, prt_client_id, orgcod, revlvl, supnum, lotnum,
         * invsts and hstacc.  However, writing a row each time will give more
         * flexibility for custom fields, and still allow us to summarize
         * when the transactions are played.  In addition, we will be able
         * to configuration what we should summarize by.
         */


        /* We need to validate that the correct unit quantities are assigned 
         * when to each detail record when we have a multiple details with 
         * unequal quantities on the same sub or load. 
         *
         * We can have two scenarios, 
         * 1. untqty was passed in - If a value is passed in and is greater 
         *   than zero, we will use that value on all details.
         * 2. untqty is NOT passed in - If untqty is not passed in, we would 
         *   have to obtain it from the record set. When adjusting load/sub 
         *   with multiple details that have  unequal untqtys untqty should not
         *   be passed in.
         *
         * One additional caveat here is, the loop and hence  the need of
         * tempqty variable. We can get into a situation where if the details 
         * dont have the same number of untqty, we have to ensure the the 
         * correct unit quantities are used. The following if-else block should 
         * help us assign correct values when looping.
         */
        
        if(tempqty > 0)
        {
            /* This is the case when a untqty was passed in. 
             * If so, use that value on all records 
             */
            untqty = ((src_adjust ? 1 : -1) * tempqty);
        }
        else if(!untqty_i)
        {
            /* This is the case when we have to 
             * get the untqty from the record set
             */
            untqty = ((src_adjust ? 1 : -1) * (sqlGetLong(res, row, "untqty")));
        }

        misTrc(T_FLOW, "Unit qty is %ld", untqty);

        if (catch_qty != 0)
        {
            catch_qty =  ((src_adjust ? 1 : -1) * (catch_qty > 0 ? catch_qty : 
                         sqlGetFloat(res, row, "catch_qty")));
        }

        misTrc(T_FLOW, "Catch qty is %lf", catch_qty);

        /*
         * For the 2007.1 release, an enhancement was made to allow users
         * to automatically send inventory adjustment transactions to the
         * host if the total changed value per part has a cost less
         * than some predefined threshold specified per warehouse.
         * To accomplish this, we'll compare the cost of the adjustment
         * (which is calculated as the product of the unit cost of the part
         * and the number of units adjusted) and the predefined threshold value.
         * If the adjustment is less than the threshold value then we won't
         * write to the invadj table at all but post the INV-ADJ transaction
         * right away.  If the cost is equal or greater than the threshold
         * value or the threshold value was null or zero then we'll just
         * write the record to the invadj table.
         */

        adjcst = labs(untqty) * sqlGetFloat(res, row, "untcst");
        if (!(sqlIsNull(res, row, "supnum")))
        {
             misTrimcpy (supnum, 
                 sqlGetString (res, row, "supnum"), SUPNUM_LEN);
        }
        cnsg_flg = sqlGetLong (res, row, "cnsg_flg");
        hld_flg = sqlGetBoolean(res, row, "hld_flg");

        if ((auto_play_cst_thr > 0) && (auto_play_cst_thr > adjcst))
        {
            misTrc(T_FLOW, "Adjustment cost %lf is less than the "
                           "threshold value %lf, post the adjustment trx now",
                           adjcst, auto_play_cst_thr);

            osSnprintf(buffer, sizeof(buffer),
                    " publish data "
                    "   where sesnum = '%s' "
                    "     and adjare = '%s' "
                    "     and untqty = %ld "
                    "     and untcas = %ld "
                    "     and prtnum = '%s' "
                    "     and prt_client_id = '%s' "
                    "     and wh_id = '%s' "
                    "     and orgcod = '%s' "
                    "     and revlvl = '%s' "
                    "     and supnum = '%s' "
                    "     and lotnum = '%s' "
                    "     and sup_lotnum = '%s' "
                    "     and mandte = '%s' "
                    "     and expire_dte = '%s' "
                    "     and inv_attr_str1 = '%s' "
                    "     and inv_attr_str2 = '%s' "
                    "     and inv_attr_str3 = '%s' "
                    "     and inv_attr_str4 = '%s' "
                    "     and inv_attr_str5 = '%s' "
                    "     and inv_attr_str6 = '%s' "
                    "     and inv_attr_str7 = '%s' "
                    "     and inv_attr_str8 = '%s' "
                    "     and inv_attr_str9 = '%s' "
                    "     and inv_attr_str10 = '%s' "
                    "     and inv_attr_str11 = '%s' "
                    "     and inv_attr_str12 = '%s' "
                    "     and inv_attr_str13 = '%s' "
                    "     and inv_attr_str14 = '%s' "
                    "     and inv_attr_str15 = '%s' "
                    "     and inv_attr_str16 = '%s' "
                    "     and inv_attr_str17 = '%s' "
                    "     and inv_attr_str18 = '%s' "
                    "     and inv_attr_int1 = %ld "
                    "     and inv_attr_int2 = %ld "
                    "     and inv_attr_int3 = %ld "
                    "     and inv_attr_int4 = %ld "
                    "     and inv_attr_int5 = %ld "
                    "     and inv_attr_flt1 = %lf "
                    "     and inv_attr_flt2 = %lf "
                    "     and inv_attr_flt3 = %lf "
                    "     and inv_attr_dte1 = '%s' "
                    "     and inv_attr_dte2 = '%s' "
                    "     and invsts = '%s' "
                    "     and catch_qty = %lf "
                    "     and hstacc = '%s' "
                    "     and adj_ref1 = '%s' "
                    "     and adj_ref2 = '%s' "
                    "     and reacod = '%s' "
                    "     and usr_id = '%s' "
                    "     and cnsg_flg = %ld "
                    "     and hld_flg = %ld "
                    "     and hld_qty = decode(@hld_flg, 0, 0, @untqty) "
                    "     and cstms_cnsgnmnt_id = '%s' "
                    "     and rttn_id = '%s' "
                    "     and cstms_bond_flg = %ld "
                    "     and dty_stmp_flg = %ld "
                    "     and attr_chg_flg = %ld "
                    " | "
                    " get integrator system id "
                    "   where systyp = 'WMD' "
                    " | "
                    " get translated warehouse ID "
                    " | "
                    " sl_log event "
                    "   where evt_id = 'INV-ADJ' "
                    "     and ifd_data_ptr = NULL "
                    "     and sys_id = @sys_id ",
                    sesnum,
                    adjare,
                    untqty,
                    untcas,
                    sqlGetString(res, row, "prtnum"),
                    sqlGetString(res, row, "prt_client_id"),
                    wh_id,
                    sqlGetString(res, row, "orgcod"),
                    sqlGetString(res, row, "revlvl"),
                    supnum,
                    sqlGetString(res, row, "lotnum"),
                    sqlGetString(res, row, "sup_lotnum"),
                    sqlGetString(res, row, "mandte"),
                    sqlGetString(res, row, "expire_dte"),
                    sqlGetString(res, row, "inv_attr_str1"),
                    sqlGetString(res, row, "inv_attr_str2"),
                    sqlGetString(res, row, "inv_attr_str3"),
                    sqlGetString(res, row, "inv_attr_str4"),
                    sqlGetString(res, row, "inv_attr_str5"),
                    sqlGetString(res, row, "inv_attr_str6"),
                    sqlGetString(res, row, "inv_attr_str7"),
                    sqlGetString(res, row, "inv_attr_str8"),
                    sqlGetString(res, row, "inv_attr_str9"),
                    sqlGetString(res, row, "inv_attr_str10"),
                    sqlGetString(res, row, "inv_attr_str11"),
                    sqlGetString(res, row, "inv_attr_str12"),
                    sqlGetString(res, row, "inv_attr_str13"),
                    sqlGetString(res, row, "inv_attr_str14"),
                    sqlGetString(res, row, "inv_attr_str15"),
                    sqlGetString(res, row, "inv_attr_str16"),
                    sqlGetString(res, row, "inv_attr_str17"),
                    sqlGetString(res, row, "inv_attr_str18"),
                    sqlGetLong(res, row, "inv_attr_int1"),
                    sqlGetLong(res, row, "inv_attr_int2"),
                    sqlGetLong(res, row, "inv_attr_int3"),
                    sqlGetLong(res, row, "inv_attr_int4"),
                    sqlGetLong(res, row, "inv_attr_int5"),
                    sqlGetFloat(res, row, "inv_attr_flt1"),
                    sqlGetFloat(res, row, "inv_attr_flt2"),
                    sqlGetFloat(res, row, "inv_attr_flt3"),
                    sqlGetString(res, row, "inv_attr_dte1"),
                    sqlGetString(res, row, "inv_attr_dte2"),
                    sqlGetString(res, row, "invsts"),
                    catch_qty,
                    strlen(hstacc) > 0 ? hstacc : sqlGetString(res, row, "invsts"),
                    adj_ref1,
                    adj_ref2,
                    reacod,
                    osGetVar(LESENV_USR_ID) ? osGetVar(LESENV_USR_ID) : "",
                    cnsg_flg,
                    hld_flg,
                    sqlGetString(res, row, "cstms_cnsgnmnt_id"),
                    sqlGetString(res, row, "rttn_id"),
                    sqlGetLong(res, row, "cstms_bond_flg"),
                    sqlGetLong(res, row, "dty_stmp_flg"),
                    attr_chg_flg);

            ret_status = srvInitiateCommand(buffer, NULL);

            if (ret_status != eOK)
            {
                free(inv_attr_int1);
                free(inv_attr_int2);
                free(inv_attr_int3);
                free(inv_attr_int4);
                free(inv_attr_int5);
                free(inv_attr_flt1);
                free(inv_attr_flt2);
                free(inv_attr_flt3);
                sqlFreeResults(res);
                return(srvResults(ret_status, NULL));
            } 
            else
            {
                CurPtr = srvResults(eOK, NULL);
            }

        }
        else
        {

            CurPtr = appCreateTableData
                     ("invadj",
                     "invadj_id",     invadj_id, COMTYP_STRING, INVADJ_ID_LEN, BOOLEAN_TRUE,
                     "sesnum",        sesnum,    COMTYP_STRING, SESNUM_LEN,    BOOLEAN_TRUE,
                     "adjare",        adjare,    COMTYP_STRING, ARECOD_LEN,    BOOLEAN_TRUE,
                     "prtnum",        sqlGetString(res, row, "prtnum"),
                                                 COMTYP_STRING, PRTNUM_LEN,    BOOLEAN_TRUE,
                     "prt_client_id", sqlGetString(res, row, "prt_client_id"),
                                                 COMTYP_STRING, CLIENT_ID_LEN, BOOLEAN_TRUE,
                     "wh_id",         wh_id,     COMTYP_STRING, WH_ID_LEN,     BOOLEAN_TRUE,
                     "orgcod",        sqlGetString(res, row, "orgcod"),
                                                 COMTYP_STRING, ORGCOD_LEN,    BOOLEAN_TRUE,
                     "revlvl",        sqlGetString(res, row, "revlvl"), 
                                                 COMTYP_STRING, REVLVL_LEN,    BOOLEAN_TRUE,
                     "supnum",        &supnum,
                                                 COMTYP_STRING, SUPNUM_LEN,    BOOLEAN_TRUE,
                     "lotnum",        sqlGetString(res, row, "lotnum"),
                                                 COMTYP_STRING, LOTNUM_LEN,    BOOLEAN_TRUE,
                     "sup_lotnum",           sqlGetString(res, row, "sup_lotnum"),
                                                 COMTYP_STRING, LOTNUM_LEN,    BOOLEAN_TRUE,
                     "mandte",               sqlGetDate(res, row, "mandte"),
                                                 COMTYP_DATTIM, MOCA_STD_DATE_LEN, BOOLEAN_TRUE,
                     "expire_dte",           sqlGetDate(res, row, "expire_dte"),
                                                 COMTYP_DATTIM, MOCA_STD_DATE_LEN, BOOLEAN_TRUE,                            
                     "inv_attr_str1",        sqlGetString(res, row, "inv_attr_str1"),
                                                 COMTYP_STRING, UDIA_STRING_LEN, BOOLEAN_TRUE,
                     "inv_attr_str2",        sqlGetString(res, row, "inv_attr_str2"),
                                                 COMTYP_STRING, UDIA_STRING_LEN, BOOLEAN_TRUE,
                     "inv_attr_str3",        sqlGetString(res, row, "inv_attr_str3"),
                                                 COMTYP_STRING, UDIA_STRING_LEN, BOOLEAN_TRUE,
                     "inv_attr_str4",        sqlGetString(res, row, "inv_attr_str4"),
                                                 COMTYP_STRING, UDIA_STRING_LEN, BOOLEAN_TRUE,
                     "inv_attr_str5",        sqlGetString(res, row, "inv_attr_str5"),
                                                 COMTYP_STRING, UDIA_STRING_LEN, BOOLEAN_TRUE,
                     "inv_attr_str6",        sqlGetString(res, row, "inv_attr_str6"),
                                                 COMTYP_STRING, UDIA_STRING_LEN, BOOLEAN_TRUE,
                     "inv_attr_str7",        sqlGetString(res, row, "inv_attr_str7"),
                                                 COMTYP_STRING, UDIA_STRING_LEN, BOOLEAN_TRUE,
                     "inv_attr_str8",        sqlGetString(res, row, "inv_attr_str8"),
                                                 COMTYP_STRING, UDIA_STRING_LEN, BOOLEAN_TRUE,
                     "inv_attr_str9",        sqlGetString(res, row, "inv_attr_str9"),
                                                 COMTYP_STRING, UDIA_STRING_LEN, BOOLEAN_TRUE,
                     "inv_attr_str10",        sqlGetString(res, row, "inv_attr_str10"),
                                                  COMTYP_STRING, UDIA_STRING_LEN, BOOLEAN_TRUE,
                     "inv_attr_str11",        sqlGetString(res, row, "inv_attr_str11"),
                                                 COMTYP_STRING, UDIA_STRING_LEN, BOOLEAN_TRUE,
                     "inv_attr_str12",        sqlGetString(res, row, "inv_attr_str12"),
                                                 COMTYP_STRING, UDIA_STRING_LEN, BOOLEAN_TRUE,
                     "inv_attr_str13",        sqlGetString(res, row, "inv_attr_str13"),
                                                 COMTYP_STRING, UDIA_STRING_LEN, BOOLEAN_TRUE,
                     "inv_attr_str14",        sqlGetString(res, row, "inv_attr_str14"),
                                                 COMTYP_STRING, UDIA_STRING_LEN, BOOLEAN_TRUE,
                     "inv_attr_str15",        sqlGetString(res, row, "inv_attr_str15"),
                                                 COMTYP_STRING, UDIA_STRING_LEN, BOOLEAN_TRUE,
                     "inv_attr_str16",        sqlGetString(res, row, "inv_attr_str16"),
                                                 COMTYP_STRING, UDIA_STRING_LEN, BOOLEAN_TRUE,
                     "inv_attr_str17",        sqlGetString(res, row, "inv_attr_str17"),
                                                 COMTYP_STRING, UDIA_STRING_LEN, BOOLEAN_TRUE,
                     "inv_attr_str18",        sqlGetString(res, row, "inv_attr_str18"),
                                                 COMTYP_STRING, UDIA_STRING_LEN, BOOLEAN_TRUE,
                     "inv_attr_int1",        inv_attr_int1,
                                                 COMTYP_LONG, sizeof(long), BOOLEAN_FALSE,
                     "inv_attr_int2",        inv_attr_int2,
                                                 COMTYP_LONG, sizeof(long), BOOLEAN_TRUE,
                     "inv_attr_int3",        inv_attr_int3,
                                                 COMTYP_LONG, sizeof(long), BOOLEAN_TRUE,
                     "inv_attr_int4",        inv_attr_int4,
                                                 COMTYP_LONG, sizeof(long), BOOLEAN_TRUE,
                     "inv_attr_int5",        inv_attr_int5,
                                                 COMTYP_LONG, sizeof(long), BOOLEAN_TRUE,
                     "inv_attr_flt1",        inv_attr_flt1,
                                                 COMTYP_FLOAT, sizeof(double), BOOLEAN_TRUE,
                     "inv_attr_flt2",        inv_attr_flt2,
                                                 COMTYP_FLOAT, sizeof(double), BOOLEAN_TRUE,
                     "inv_attr_flt3",        inv_attr_flt3,
                                                 COMTYP_FLOAT, sizeof(double), BOOLEAN_TRUE,
                     "inv_attr_dte1",        sqlGetDate(res, row, "inv_attr_dte1"),
                                                 COMTYP_DATTIM, MOCA_STD_DATE_LEN, BOOLEAN_TRUE,
                     "inv_attr_dte2",        sqlGetDate(res, row, "inv_attr_dte2"),
                                                 COMTYP_DATTIM, MOCA_STD_DATE_LEN, BOOLEAN_TRUE,
                     "invsts",        sqlGetString(res, row, "invsts"), 
                                                 COMTYP_STRING, INVSTS_LEN,    BOOLEAN_TRUE,
                     "untqty",        &untqty,   COMTYP_INT,    sizeof(long),  BOOLEAN_FALSE,
                     "catch_qty",     &catch_qty,COMTYP_FLOAT,  sizeof(double), BOOLEAN_FALSE,
                     "hstacc", strlen(hstacc) > 0 ? hstacc : sqlGetString(res, row, "invsts"),
                                                 COMTYP_STRING, HSTACC_LEN,    BOOLEAN_TRUE,
                     "adj_ref1",     adj_ref1,   COMTYP_STRING, ADJ_REF1_LEN,  BOOLEAN_TRUE,
                     "adj_ref2",     adj_ref2,   COMTYP_STRING, ADJ_REF2_LEN,  BOOLEAN_TRUE,
                     "reacod",       reacod,     COMTYP_STRING, REACOD_LEN,    BOOLEAN_TRUE,
                     "usr_id",        osGetVar(LESENV_USR_ID) ? osGetVar(LESENV_USR_ID) : "",
                                                 COMTYP_STRING, USR_ID_LEN,    BOOLEAN_TRUE,
                     "moddte",   "sysdate", COMTYP_DATTIM, DB_STD_DATE_LEN, BOOLEAN_FALSE,
                     "cnsg_flg",     &cnsg_flg, COMTYP_INT, sizeof(long), BOOLEAN_FALSE,
                     "hld_flg",      &hld_flg, COMTYP_INT, sizeof(long), BOOLEAN_FALSE,
                     "rttn_id",      rttn_id, COMTYP_STRING, RTTN_ID_LEN, BOOLEAN_TRUE,
                     "cstms_cnsgnmnt_id", cstms_cnsgnmnt_id, COMTYP_STRING, CSTMS_CNSGNMNT_ID_LEN, BOOLEAN_TRUE,
                     "cstms_bond_flg", &cstms_bond_flg, COMTYP_LONG, sizeof(long), BOOLEAN_FALSE,
                     "dty_stmp_flg", &dty_stmp_flg, COMTYP_LONG, sizeof(long), BOOLEAN_FALSE,
                     "untcas", &untcas, COMTYP_INT, sizeof(long), BOOLEAN_FALSE,
                     NULL);
        }
    }

    free(inv_attr_int1);
    free(inv_attr_int2);
    free(inv_attr_int3);
    free(inv_attr_int4);
    free(inv_attr_int5);
    free(inv_attr_flt1);
    free(inv_attr_flt2);
    free(inv_attr_flt3);
    sqlFreeResults(res);
    return (CurPtr);
}
