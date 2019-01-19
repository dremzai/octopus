# Generated by Django 2.0.6 on 2018-07-19 07:41

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('octopus_spark', '0003_auto_20180717_1427'),
    ]

    operations = [
        migrations.AlterField(
            model_name='extr',
            name='c_dt',
            field=models.CharField(default='', max_length=8, verbose_name='创建日期'),
        ),
        migrations.AlterField(
            model_name='extr',
            name='c_oper_no',
            field=models.CharField(default='', max_length=30, verbose_name='创建人员'),
        ),
        migrations.AlterField(
            model_name='extr',
            name='end_dt',
            field=models.CharField(default='', max_length=8, verbose_name='失效日期'),
        ),
        migrations.AlterField(
            model_name='extr',
            name='extr_condition',
            field=models.CharField(default='', max_length=200, verbose_name='抽取条件'),
        ),
        migrations.AlterField(
            model_name='extr',
            name='extr_freq',
            field=models.CharField(default='', max_length=30, verbose_name='抽取频度'),
        ),
        migrations.AlterField(
            model_name='extr',
            name='extr_no',
            field=models.CharField(default='', max_length=30, verbose_name='序号'),
        ),
        migrations.AlterField(
            model_name='extr',
            name='m_dt',
            field=models.CharField(default='', max_length=8, verbose_name='修改日期'),
        ),
        migrations.AlterField(
            model_name='extr',
            name='m_oper_no',
            field=models.CharField(default='', max_length=30, verbose_name='修改人员'),
        ),
        migrations.AlterField(
            model_name='extr',
            name='save_freq',
            field=models.CharField(default='', max_length=30, verbose_name='保留周期'),
        ),
        migrations.AlterField(
            model_name='extr',
            name='src_sys',
            field=models.CharField(default='', max_length=30, verbose_name='源系统编号'),
        ),
        migrations.AlterField(
            model_name='extr',
            name='src_tab_nm',
            field=models.CharField(default='', max_length=30, verbose_name='源系统表名'),
        ),
        migrations.AlterField(
            model_name='extr',
            name='start_dt',
            field=models.CharField(default='', max_length=8, verbose_name='生效日期'),
        ),
        migrations.AlterField(
            model_name='srcdb',
            name='c_dt',
            field=models.CharField(default='', max_length=8, verbose_name='创建日期'),
        ),
        migrations.AlterField(
            model_name='srcdb',
            name='c_oper_no',
            field=models.CharField(default='', max_length=30, verbose_name='创建人员'),
        ),
        migrations.AlterField(
            model_name='srcdb',
            name='end_dt',
            field=models.CharField(default='', max_length=8, verbose_name='失效日期'),
        ),
        migrations.AlterField(
            model_name='srcdb',
            name='m_dt',
            field=models.CharField(default='', max_length=8, verbose_name='修改日期'),
        ),
        migrations.AlterField(
            model_name='srcdb',
            name='m_oper_no',
            field=models.CharField(default='', max_length=30, verbose_name='修改人员'),
        ),
        migrations.AlterField(
            model_name='srcdb',
            name='src_instance',
            field=models.CharField(default='', max_length=50, verbose_name='实例名称'),
        ),
        migrations.AlterField(
            model_name='srcdb',
            name='src_ip',
            field=models.CharField(default='', max_length=50, verbose_name='IP地址'),
        ),
        migrations.AlterField(
            model_name='srcdb',
            name='src_nm',
            field=models.CharField(default='', max_length=200, verbose_name='源系统名称'),
        ),
        migrations.AlterField(
            model_name='srcdb',
            name='src_port',
            field=models.CharField(default='', max_length=50, verbose_name='端口'),
        ),
        migrations.AlterField(
            model_name='srcdb',
            name='src_pwd',
            field=models.CharField(default='', max_length=50, verbose_name='密码'),
        ),
        migrations.AlterField(
            model_name='srcdb',
            name='src_sys',
            field=models.CharField(default='', max_length=30, verbose_name='源系统编号'),
        ),
        migrations.AlterField(
            model_name='srcdb',
            name='src_type',
            field=models.CharField(default='', max_length=50, verbose_name='数据库类型'),
        ),
        migrations.AlterField(
            model_name='srcdb',
            name='src_user',
            field=models.CharField(default='', max_length=50, verbose_name='用户名称'),
        ),
        migrations.AlterField(
            model_name='srcdb',
            name='start_dt',
            field=models.CharField(default='', max_length=8, verbose_name='生效日期'),
        ),
    ]