import xadmin
from octopus_spark import models

class ExtrAdmin:
    list_display = ['id', 'src_sys', 'src_tab_nm', 'extr_condition', 'extr_data_dt', 'extr_cnt', 'extr_freq', 'save_freq',
                    'c_oper_no', 'c_dt']
    list_editable = ['src_sys', 'src_tab_nm', 'extr_condition', 'extr_data_dt', 'extr_cnt', 'extr_freq', 'save_freq',
                     'c_oper_no', 'c_dt']
    search_fields = ['src_sys', 'c_oper_no']
    list_filter = ['src_sys', 'c_oper_no']
    # list_display = ['extr_no', 'src_sys', 'src_tab_nm', 'extr_condition', 'extr_data_dt', 'extr_freq', 'save_freq',
    #                 'start_dt', 'end_dt', 'c_oper_no', 'c_dt', 'm_oper_no', 'm_dt']
    # list_editable = ['extr_no', 'src_sys', 'src_tab_nm', 'extr_condition', 'extr_data_dt', 'extr_freq', 'save_freq',
    #                  'start_dt', 'end_dt', 'c_oper_no', 'c_dt', 'm_oper_no', 'm_dt']
    # search_fields = ['src_sys']
    # list_filter = ['src_sys']

xadmin.site.register(models.Extr, ExtrAdmin)

class SrcDBAdmin:
    list_display = ['src_sys', 'src_nm', 'src_type', 'src_part', 'src_user', 'src_pwd', 'src_ip', 'src_port',
                    'src_instance', 'start_dt', 'end_dt', 'c_oper_no', 'c_dt', 'm_oper_no', 'm_dt']
    list_editable = ['src_sys', 'src_nm', 'src_type', 'src_part', 'src_user', 'src_pwd', 'src_ip', 'src_port',
                    'src_instance', 'start_dt', 'end_dt', 'c_oper_no', 'c_dt', 'm_oper_no', 'm_dt']
    search_fields = ['src_sys']
    list_filter = ['src_sys']

xadmin.site.register(models.SrcDB, SrcDBAdmin)

class ParAdmin:
    list_display = ['par_type','par_type_nm','par_cd','par_nm','f_par_cd','par_lv'
        ,'start_dt','end_dt','c_oper_no','c_dt','m_oper_no','m_dt']
    list_editable = ['par_type','par_type_nm','par_cd','par_nm','f_par_cd','par_lv'
        ,'start_dt','end_dt','c_oper_no','c_dt','m_oper_no','m_dt']

xadmin.site.register(models.Par, ParAdmin)

class DefDimAdmin:
    list_display = [#'dim_no','dim_desc',
                    'dim_no', 'dim_cd','dim_nm','f_dim_cd','dim_lv']
                    #,'remark','bel_depo_no','bel_oper_no'
                    #,'dim_src','start_dt','end_dt','c_oper_no','c_dt'
                    #,'m_oper_no','m_dt']
    list_editable = [#'dim_no','dim_desc',
                    'dim_no', 'dim_cd','dim_nm','f_dim_cd','dim_lv']
                    #,'remark','bel_depo_no','bel_oper_no'
                    #,'dim_src','start_dt','end_dt','c_oper_no','c_dt'
                    #,'m_oper_no','m_dt']

xadmin.site.register(models.DefDim, DefDimAdmin)

class DefIndAdmin:
    list_display = ['ind_no','ind_nm', 'ind_data_dt','ind_prop','ind_freq','ind_biz_type','ind_src'
        ,'ind_biz_cali_desc','ind_biz_cali_file','ind_biz_cali_file_nm','ind_tech_cali_desc'
        ,'ind_tech_cali_file','ind_tech_cali_file_nm','ind_version','start_dt','end_dt'
        ,'bel_depo_no','bel_oper_no','c_oper_no','c_dt','m_oper_no']
    list_editable = ['ind_no','ind_nm', 'ind_data_dt','ind_prop','ind_freq','ind_biz_type','ind_src'
        ,'ind_biz_cali_desc','ind_biz_cali_file','ind_biz_cali_file_nm','ind_tech_cali_desc'
        ,'ind_tech_cali_file','ind_tech_cali_file_nm','ind_version','start_dt','end_dt'
        ,'bel_depo_no','bel_oper_no','c_oper_no','c_dt','m_oper_no']

xadmin.site.register(models.DefInd, DefIndAdmin)

class IndDimRelaAdmin:
    list_display = ['ind_no','dim_no','start_dt','end_dt','c_oper_no','c_dt'
        ,'m_oper_no','m_dt']
    list_editable = ['ind_no','dim_no','start_dt','end_dt','c_oper_no','c_dt'
        ,'m_oper_no','m_dt']
xadmin.site.register(models.IndDimRela, IndDimRelaAdmin)


class DefIndCalcAdmin:
    list_display = ['ind_no', 'ind_proc', 'tmp_tab', 'calc_lvl']
    list_editable = ['ind_no', 'ind_proc', 'tmp_tab', 'calc_lvl']
    search_fields = ['ind_no']
    list_filter = ['ind_no']
xadmin.site.register(models.DefIndCalc, DefIndCalcAdmin)


    # class HbaseCatelogAdmin:
#     list_display = ['namespace', 'name', 'cf', 'col', 'type']
#     list_editable = ['namespace', 'name', 'cf', 'col', 'type']
#     search_fields = ['namespace', 'name', 'cf', 'col', 'type']
#     list_filter = ['namespace', 'name', 'cf', 'col', 'type']
#
# xadmin.site.register(models.HbaseCatelog, HbaseCatelogAdmin)