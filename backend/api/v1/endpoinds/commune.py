from fastapi import APIRouter, status, HTTPException, Depends


from schemas.commune import CommuneCreate, CommuneOut
from deps import get_db
from crud.commune import create_commune
from sqlalchemy.orm import Session

router = APIRouter()


@router.post("/commune", status_code=status.HTTP_200_OK, response_model=CommuneOut)
def post_commune(commune: CommuneCreate, db: Session = Depends(get_db)):
    db_commune = create_commune(db, commune)

    if not db_commune:
        raise HTTPException(detail="erreur lors de la création", code=500)
    return db_commune

# @router.get("/communes/{name}")
# def get_commune(name: str, db: Session = Depends(get_db)):
#     db_commune = get_commune_by_name(name)

#     if not db_commune:
#         raise HTTPException(detail="erreur lors de la création", status_code=status.HTTP_404_NOT_FOUND)

#     return db_commune