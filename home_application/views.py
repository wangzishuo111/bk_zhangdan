# -*- coding: utf-8 -*-
from django.shortcuts import render, redirect


# 开发框架中通过中间件默认是需要登录态的，如有不需要登录的，可添加装饰器login_exempt
# 装饰器引入 from blueapps.account.decorators import login_exempt
def home(request):
    return redirect('http://grafana.infra.growingio.com/d/Q0qq2iPWk/month-bill?orgId=1&from=now%2Fw&to=now%2Fw')


def contact(request):
    """
    联系我们
    """
    return render(request, 'home_application/contact.html')
