from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from agent.mock_mcp import agent

router = APIRouter()

class ChatRequest(BaseModel):
    message: str

@router.post("/chat")
def chat_with_data(request: ChatRequest):
    """
    Endpoint que recibe texto y devuelve instrucciones para el mapa.
    """
    try:
        response = agent.process_query(request.message)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))