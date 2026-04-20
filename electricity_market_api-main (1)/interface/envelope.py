from pydantic import BaseModel, Field
from typing import Generic, TypeVar, Optional, get_type_hints

# Define a type variable
T = TypeVar('T')

# Define a generic envelope model
class Envelope(BaseModel, Generic[T]):
    State: int
    Data: Optional[T] = None
    Log: Optional[str] = None
    ErrorCode: Optional[int] = None

# Function to get a success or warning envelope
def getSuccessEnvelope(data: T, warning: bool = False, log: str = None) -> Envelope[T]:
    return Envelope[T](State=2 if warning else 1, Data=data, Log=log)

# Function to get a failure envelope
def getFailedEnvelope(data: T = None, errorCode: int = None, message: str = None) -> Envelope[T]:
    return Envelope[T](State=3, Data=data, ErrorCode=errorCode, Log=message)
