from fastapi import APIRouter
from api.v1.endpoinds import commune

api_v1 = APIRouter()
api_v1.include_router(commune.router, prefix="/commune", tags=["commune"])

