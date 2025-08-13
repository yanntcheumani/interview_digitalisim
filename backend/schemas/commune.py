from pydantic import BaseModel



class CommuneCreate(BaseModel):
    Name: str
    PostalCode: int
    department: int

class CommuneOut(CommuneCreate):
    uuid: str

    class config:
        ord_mode =  True
