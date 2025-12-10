from django.urls import path
from . import views

app_name = "taxonomy_ui"

urlpatterns = [
    path("", views.home, name="home"),   # ✅ THIS IS MISSING
    path("upload/", views.upload_and_process, name="upload_and_process"),
    path("parts/", views.part_list, name="part_list"),
    path("refresh-stage1/", views.run_stage1_refresh, name="run_stage1_refresh"),
    path("download-full/<str:filename>/", views.download_full_output, name="download_full_output"),
    path("download-selected/", views.download_selected_columns, name="download_selected_columns"),
]
