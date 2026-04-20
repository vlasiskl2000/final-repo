from models.file import CustomFile
from typing import Generic, TypeVar, Optional
from pydantic import BaseModel

class Metadata(BaseModel):
    Success: Optional[bool] = True
    Log: Optional[str] = None
    FileMetadataId: Optional[int] = 0

# Generic type for results
T = TypeVar('T')

# Model for Parsing Metadata Payload
class ParsingMetadataPayload(BaseModel, Generic[T]):
    Results: T
    ParsingMetadata: Metadata

# Model for Virtual File Metadata Payload
class VirtualFileMetadataPayload(BaseModel, Generic[T]):
    Results: T
    ParsingMetadata: Metadata
    File: CustomFile

# Function to get Metadata
def getMetadata(success: bool, file_id: int, log: str = None) -> Metadata:
    return Metadata(Success=success, Log=log, FileMetadataId=file_id)

# Function to get Parsing Metadata Payload
def getParsingMetadataPayload(results: T, metadata: Metadata) -> ParsingMetadataPayload[T]:
    return ParsingMetadataPayload(Results=results, ParsingMetadata=metadata)

# Function to get Virtual File Metadata
def getVirtualFileMetadata(results: T, metadata: Metadata, file: CustomFile) -> VirtualFileMetadataPayload[T]:
    return VirtualFileMetadataPayload(Results=results, ParsingMetadata=metadata, File=file)