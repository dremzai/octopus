# Generated by Django 2.0.6 on 2018-10-09 02:49

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('octopus_spark', '0012_defindcalc_ind_data_dt'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='defindcalc',
            name='ind_data_dt',
        ),
        migrations.AddField(
            model_name='defind',
            name='ind_data_dt',
            field=models.CharField(default='', max_length=10, verbose_name='数据日期'),
        ),
    ]
