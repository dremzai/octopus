from django.db import models

# Create your models here.

# python manage.py makemigrations
# python manage.py migrate


class HbaseCatelog(models.Model):
    namespace = models.CharField(max_length=50, verbose_name='tableNamespace', default="")
    name = models.CharField(max_length=50, verbose_name='tableName', default="")
    cf = models.CharField(max_length=30, verbose_name='columnFamily', default="")
    col = models.CharField(max_length=30, verbose_name='columnName', default="")
    type = models.CharField(max_length=30, verbose_name='columnType', default="")
    class Meta:
        verbose_name = "配置表-HBASECATELOG"
        verbose_name_plural = verbose_name

class SrcDB(models.Model):
    src_sys=models.CharField(max_length=30, verbose_name='源系统编号', default="")
    src_nm=models.CharField(max_length=200, verbose_name='源系统名称', default="")
    src_type=models.CharField(max_length=50, verbose_name='数据库类型', default="")
    src_part=models.CharField(max_length=3, verbose_name='抽取并行数', default="")
    src_user=models.CharField(max_length=50, verbose_name='用户名称', default="")
    src_pwd=models.CharField(max_length=50, verbose_name='密码', default="")
    src_ip=models.CharField(max_length=50, verbose_name='IP地址', default="")
    src_port=models.CharField(max_length=50, verbose_name='端口', default="")
    src_instance=models.CharField(max_length=50, verbose_name='实例名称', default="")
    start_dt=models.CharField(max_length=8, verbose_name='生效日期', default="")
    end_dt=models.CharField(max_length=8, verbose_name='失效日期', default="")
    c_oper_no=models.CharField(max_length=30, verbose_name='创建人员', default="")
    c_dt=models.CharField(max_length=8, verbose_name='创建日期', default="")
    m_oper_no=models.CharField(max_length=30, verbose_name='修改人员', default="")
    m_dt=models.CharField(max_length=8, verbose_name='修改日期', default="")
    class Meta:
        verbose_name = "数据源"
        verbose_name_plural = verbose_name

class Extr(models.Model):
    # extr_no = models.CharField(max_length=30, verbose_name='序号', default="")
    src_sys = models.CharField(max_length=30, verbose_name='源系统编号', default="")
    src_tab_nm = models.CharField(max_length=30, verbose_name='源系统表名', default="")
    extr_condition = models.CharField(max_length=200, verbose_name='抽取条件', default="")
    extr_data_dt = models.CharField(max_length=8, verbose_name='数据日期', default="")
    extr_cnt = models.CharField(max_length=30, verbose_name='记录条数', default="")
    extr_freq = models.CharField(max_length=30, verbose_name='抽取频度', default="")
    save_freq = models.CharField(max_length=30, verbose_name='保留周期', default="")
    # start_dt = models.CharField(max_length=8, verbose_name='生效日期', default="")
    # end_dt = models.CharField(max_length=8, verbose_name='失效日期', default="")
    c_oper_no = models.CharField(max_length=30, verbose_name='创建人员', default="")
    c_dt = models.CharField(max_length=8, verbose_name='创建日期', default="")
    # m_oper_no = models.CharField(max_length=30, verbose_name='修改人员', default="")
    # m_dt = models.CharField(max_length=8, verbose_name='修改日期', default="")
    class Meta:
        verbose_name = "抽取表"
        verbose_name_plural = verbose_name

class Par(models.Model):
    par_type = models.CharField(max_length=30, verbose_name='参数类型', default="")
    par_type_nm = models.CharField(max_length=200, verbose_name='参数类型描述', default="")
    par_cd = models.CharField(max_length=30, verbose_name='参数代码', default="")
    par_nm = models.CharField(max_length=200, verbose_name='参数描述', default="")
    f_par_cd = models.CharField(max_length=30, verbose_name='父参数代码', default="")
    par_lv = models.CharField(max_length=10, verbose_name='参数层级', default="")
    start_dt = models.CharField(max_length=8, verbose_name='生效日期', default="")
    end_dt = models.CharField(max_length=8, verbose_name='失效日期', default="")
    c_oper_no = models.CharField(max_length=30, verbose_name='创建人员', default="")
    c_dt = models.CharField(max_length=8, verbose_name='创建时间', default="")
    m_oper_no = models.CharField(max_length=30, verbose_name='修改人员', default="")
    m_dt = models.CharField(max_length=8, verbose_name='修改时间', default="")
    class Meta:
        verbose_name = "参数表"
        verbose_name_plural = verbose_name

class DefDim(models.Model):
    dim_no=models.CharField(max_length=30, verbose_name='维度编号', default="")
    # dim_desc=models.CharField(max_length=200, verbose_name='维度名称', default="")
    dim_cd=models.CharField(max_length=30, verbose_name='维度代码', default="")
    dim_nm=models.CharField(max_length=200, verbose_name='维度名称', default="")
    f_dim_cd=models.CharField(max_length=30, verbose_name='父维度代码', default="")
    dim_lv=models.CharField(max_length=10, verbose_name='维度层级', default="")
    # remark=models.CharField(max_length=500, verbose_name='备注', default="")
    # bel_depo_no=models.CharField(max_length=30, verbose_name='归属部门', default="")
    # bel_oper_no=models.CharField(max_length=30, verbose_name='归属人员', default="")
    # dim_src=models.CharField(max_length=30, verbose_name='维度来源', default="")
    # start_dt=models.CharField(max_length=8, verbose_name='生效日期', default="")
    # end_dt=models.CharField(max_length=8, verbose_name='失效日期', default="")
    # c_oper_no=models.CharField(max_length=30, verbose_name='创建人员', default="")
    # c_dt=models.CharField(max_length=8, verbose_name='创建时间', default="")
    # m_oper_no=models.CharField(max_length=30, verbose_name='修改人员', default="")
    # m_dt=models.CharField(max_length=8, verbose_name='修改时间', default="")
    class Meta:
        verbose_name = "维度表"
        verbose_name_plural = verbose_name

class DefInd(models.Model):
    ind_no=models.CharField(max_length=30, verbose_name='指标编号', default="")
    ind_nm=models.CharField(max_length=200, verbose_name='指标名称', default="")
    ind_data_dt = models.CharField(max_length=8, verbose_name='数据日期', default="")
    ind_prop=models.CharField(max_length=30, verbose_name='指标属性', default="")
    ind_freq=models.CharField(max_length=30, verbose_name='指标频度', default="")
    ind_biz_type=models.CharField(max_length=30, verbose_name='业务类型', default="")
    ind_src=models.CharField(max_length=100, verbose_name='指标来源', default="")
    ind_biz_cali_desc=models.CharField(max_length=3000, verbose_name='业务口径描述', default="")
    ind_biz_cali_file=models.CharField(max_length=3000, verbose_name='业务口径附件', default="")
    ind_biz_cali_file_nm=models.CharField(max_length=200, verbose_name='业务口径附件名称', default="")
    ind_tech_cali_desc=models.CharField(max_length=3000, verbose_name='技术口径描述', default="")
    ind_tech_cali_file=models.CharField(max_length=3000, verbose_name='技术口径附件', default="")
    ind_tech_cali_file_nm=models.CharField(max_length=200, verbose_name='技术口径附件名称', default="")
    ind_version=models.CharField(max_length=30, verbose_name='指标版本号', default="")
    start_dt=models.CharField(max_length=8, verbose_name='生效日期', default="")
    end_dt=models.CharField(max_length=8, verbose_name='失效日期', default="")
    bel_depo_no=models.CharField(max_length=30, verbose_name='归属部门', default="")
    bel_oper_no=models.CharField(max_length=30, verbose_name='归属人员', default="")
    c_oper_no=models.CharField(max_length=30, verbose_name='创建人员', default="")
    c_dt=models.CharField(max_length=8, verbose_name='创建日期', default="")
    m_oper_no=models.CharField(max_length=30, verbose_name='修改人员', default="")
    class Meta:
        verbose_name = "指标表"
        verbose_name_plural = verbose_name

class IndDimRela(models.Model):
    ind_no=models.CharField(max_length=30, verbose_name='指标编号', default="")
    dim_no=models.CharField(max_length=30, verbose_name='维度编号', default="")
    start_dt=models.CharField(max_length=8, verbose_name='生效日期', default="")
    end_dt=models.CharField(max_length=8, verbose_name='失效日期', default="")
    c_oper_no=models.CharField(max_length=30, verbose_name='创建人员', default="")
    c_dt=models.CharField(max_length=8, verbose_name='创建日期', default="")
    m_oper_no=models.CharField(max_length=30, verbose_name='修改人员', default="")
    m_dt=models.CharField(max_length=8, verbose_name='修改日期', default="")
    class Meta:
        verbose_name = "指标维度"
        verbose_name_plural = verbose_name


class DefIndCalc(models.Model):
    ind_no = models.CharField(max_length=30, verbose_name='指标编号', default="")
    ind_proc = models.TextField(verbose_name='处理语句', default="")
    tmp_tab = models.CharField(max_length=200, verbose_name='临时表名', default="")
    calc_lvl = models.IntegerField(verbose_name='计算层级', default="")
    class Meta:
        verbose_name = "指标计算表"
        verbose_name_plural = verbose_name



