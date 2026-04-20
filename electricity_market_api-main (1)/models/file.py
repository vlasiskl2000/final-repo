from typing import Optional, Union, Type
from pydantic import BaseModel
from fastapi import File, Form, UploadFile

import inspect
from datetime import date, datetime

class CustomFile(BaseModel):
    Id: int
    FileName: str
    FileDescription: str
    FileType: str
    Version: Optional[int] = None
    Url: Optional[str] = None
    PublicationDate: Optional[datetime] = None
    TargetDateFrom: Union[datetime, date]
    TargetDateTo: Union[datetime, date]
    ByUser: Optional[bool] = False
    Success: Optional[bool] = True
    StatusCode: Optional[int] = None
    Log: Optional[str] = None

def as_form(
    id: int = Form(...),
    file_name: str = Form(...),
    file_description: str = Form(...),
    file_type: str = Form(...),
    version: Optional[int] = Form(None),
    url: Optional[str] = Form(None),
    publication_date: Optional[datetime] = Form(None),
    target_date_from: Union[datetime, date] = Form(...),
    target_date_to: Union[datetime, date] = Form(...),
    by_user: Optional[bool] = Form(False),
    success: Optional[bool] = Form(True),
    status_code: Optional[int] = Form(None),
    log: Optional[str] = Form(None)
) -> CustomFile:
    return CustomFile(
        id=id,
        file_name=file_name,
        file_description=file_description,
        file_type=file_type,
        version=version,
        url=url,
        publication_date=publication_date,
        target_date_from=target_date_from,
        target_date_to=target_date_to,
        by_user=by_user,
        success=success,
        status_code=status_code,
        log=log
    )