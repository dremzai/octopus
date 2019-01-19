from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    # path('spark/', views.spark, name='spark'),
    path('test/<str:aaa>/', views.test, name='test'),
    path('execurlsql/<str:urlSql>/', views.execUrlSql, name='execurlsql'),
    path('execsql/', views.execInputSql, name='execsql'),
    path('execessql/', views.execInputESSql, name='execessql'),
    # path('extrdata/<str:extrSystem>/<str:extrTable>/<str:extrConditon>', views.extrDataCond, name='extrdatacond'),
    path('extrdata/<str:extrSystem>/<str:extrTable>', views.extrData, name='extrdata'),
    path('extrdata/<str:extrSystem>/<str:extrTable>/<str:extrDate>', views.extrData, name='extrdatadate'),
    path('ind/<str:indNo>', views.indProc, name='indproc'),
    path('ind/<str:indNo>/<str:indDate>', views.indProc, name='indprocdate'),
    path('refdim', views.refreshDefDim, name='refdim'),
    path('search', views.search, name='search'),
    path('execpythoncmd/', views.execInputPythonCmd, name='pythoncmd'),
    # path('hello', views.hello, name='hello'),
]
#
# The following path converters are available by default:
#
# str - Matches any non-empty string, excluding the path separator, '/'. This is the default if a converter isn’t included in the expression.
# int - Matches zero or any positive integer. Returns an int.
# slug - Matches any slug string consisting of ASCII letters or numbers, plus the hyphen and underscore characters. For example, building-your-1st-django-site.
# uuid - Matches a formatted UUID. To prevent multiple URLs from mapping to the same page, dashes must be included and letters must be lowercase. For example, 075194d3-6885-417e-a8a8-6c931e272f00. Returns a UUID instance.
# path - Matches any non-empty string, including the path separator, '/'. This allows you to match against a complete URL path rather than just a segment of a URL path as with str.