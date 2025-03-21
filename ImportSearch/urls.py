"""
URL configuration for ImportSearch project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from SearchApp import views

urlpatterns = [
    path('admin/', admin.site.urls),
    # path('', views.home, name='search-page'),
    path('', views.search, name='search'),
    path('results/', views.results, name='results'),
    path('insert/', views.insert, name='insert'),
    path('search_bom/', views.search_bom, name="search_bom"),
    path('loading/<int:task_id>/', views.loading, name='loading'),
    path('progress_status/<int:task_id>/', views.progress_status, name='progress_status'),
]
