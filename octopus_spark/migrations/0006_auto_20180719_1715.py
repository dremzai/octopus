# Generated by Django 2.0.6 on 2018-07-19 09:15

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('octopus_spark', '0005_auto_20180719_1656'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='defdim',
            name='bel_depo_no',
        ),
        migrations.RemoveField(
            model_name='defdim',
            name='bel_oper_no',
        ),
        migrations.RemoveField(
            model_name='defdim',
            name='c_dt',
        ),
        migrations.RemoveField(
            model_name='defdim',
            name='c_oper_no',
        ),
        migrations.RemoveField(
            model_name='defdim',
            name='dim_desc',
        ),
        migrations.RemoveField(
            model_name='defdim',
            name='dim_no',
        ),
        migrations.RemoveField(
            model_name='defdim',
            name='dim_src',
        ),
        migrations.RemoveField(
            model_name='defdim',
            name='end_dt',
        ),
        migrations.RemoveField(
            model_name='defdim',
            name='m_dt',
        ),
        migrations.RemoveField(
            model_name='defdim',
            name='m_oper_no',
        ),
        migrations.RemoveField(
            model_name='defdim',
            name='remark',
        ),
        migrations.RemoveField(
            model_name='defdim',
            name='start_dt',
        ),
    ]
