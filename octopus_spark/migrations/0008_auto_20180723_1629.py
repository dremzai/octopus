# Generated by Django 2.0.6 on 2018-07-23 08:29

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('octopus_spark', '0007_defindcalc'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='defindcalc',
            options={'verbose_name': '指标计算表', 'verbose_name_plural': '指标计算表'},
        ),
        migrations.AddField(
            model_name='defdim',
            name='dim_no',
            field=models.CharField(default='', max_length=30, verbose_name='维度编号'),
        ),
    ]