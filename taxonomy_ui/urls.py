from django.urls import path
from . import views

app_name = "taxonomy_ui"

urlpatterns = [
    path("", views.home, name="home"),
    path("parts/", views.part_list, name="part_list"),
    path("upload/", views.upload_and_process, name="upload"),

    # Downloads
    path("download-selected/", views.download_selected_columns, name="download_selected"),
    path("download-full/<str:filename>/", views.download_full_output, name="download_full"),

    # Refresh Stage1
    path("refresh-stage1/", views.run_stage1_refresh, name="refresh_stage1"),
]
