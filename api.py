from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from elasticsearch import AsyncElasticsearch
from typing import Optional, List
import uvicorn
import os

app = FastAPI()

# Initialiser le client Elasticsearch
ELASTIC_HOST = os.getenv(
    "ELASTIC_HOST", "elasticsearch"
)  # Utiliser le nom du conteneur Elasticsearch dans le host
ELASTIC_PORT = os.getenv("ELASTIC_PORT", "9200")
ELASTIC_USERNAME = os.getenv("ELASTIC_USERNAME", "elastic")
ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD", "")

es = AsyncElasticsearch(
    hosts=[f"http://{ELASTIC_HOST}:{ELASTIC_PORT}"],
    http_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD),
)


# Pydantic model pour l'offre d'emploi
class JobOffer(BaseModel):
    id: Optional[str] = None
    Job: Optional[str] = None
    Company: Optional[str] = None
    Address: Optional[str] = None
    Date: Optional[str] = None
    Link: Optional[str] = None
    Salary: Optional[float] = None
    Description: Optional[str] = None

    class Config:
        orm_mode = True  # Permet de sérialiser les objets


@app.get("/")
async def read_root():
    """
    Endpoint racine pour vérifier que l'API fonctionne.

    Returns:
        dict: Message de bienvenue.
    """
    return {"message": "Bienvenue sur l'API des offres d'emploi"}


@app.post("/jobs/", response_model=JobOffer)
async def create_job_offer(job_offer: JobOffer):
    """
    Crée une nouvelle offre d'emploi dans l'index Elasticsearch.

    Parameters:
    - job_offer: JobOffer - Les détails de l'offre d'emploi.

    Returns:
    - Les détails de l'offre d'emploi créée, y compris l'ID.
    """
    try:
        # Exclure l'ID lors de l'indexation
        job_offer_data = job_offer.dict(exclude={"id"})

        # Indexer l'offre d'emploi dans Elasticsearch
        response = await es.index(
            index="offres_emploi4", body=job_offer_data, refresh=True
        )

        # Récupérer l'ID généré par Elasticsearch
        job_offer_id = response["_id"]

        # Retourner les détails de l'offre d'emploi avec l'ID
        return JobOffer(**job_offer_data, id=job_offer_id)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/jobs/{job_id}", response_model=JobOffer)
async def read_job_offer(job_id: str):
    """
    Récupère une offre d'emploi par son ID depuis Elasticsearch.

    Parameters:
    - job_id: str - L'ID de l'offre d'emploi.

    Returns:
    - Les détails de l'offre d'emploi incluant l'ID.
    """
    try:
        response = await es.get(index="offres_emploi4", id=job_id)
        if response["found"]:
            job_offer_data = response["_source"]
            return JobOffer(**job_offer_data, id=response["_id"])
        else:
            raise HTTPException(status_code=404, detail="Job offer not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/jobs/{job_id}", response_model=JobOffer)
async def update_job_offer(job_id: str, job_offer: JobOffer):
    """
    Met à jour une offre d'emploi existante dans l'index Elasticsearch.

    Parameters:
    - job_id: str - L'ID de l'offre d'emploi à mettre à jour.
    - job_offer: JobOffer - Les détails mis à jour de l'offre d'emploi.

    Returns:
    - Les détails mis à jour de l'offre d'emploi.
    """
    try:
        job_offer_data = job_offer.dict(exclude={"id"}, exclude_unset=True)

        await es.update(
            index="offres_emploi4",
            id=job_id,
            body={"doc": job_offer_data},
        )
        return JobOffer(**job_offer_data, id=job_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/jobs/{job_id}")
async def delete_job_offer(job_id: str):
    """
    Supprime une offre d'emploi par son ID depuis Elasticsearch.

    Parameters:
    - job_id: str - L'ID de l'offre d'emploi à supprimer.

    Returns:
    - Un message indiquant l'état de la suppression.
    """
    try:
        await es.delete(index="offres_emploi4", id=job_id)
        return {"message": "Job offer deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/jobs/", response_model=List[JobOffer])
async def search_job_offers(query: Optional[str] = None):
    """
    Recherche des offres d'emploi dans Elasticsearch.

    Parameters:
    - query: Optional[str] - La requête de recherche.

    Returns:
    - Une liste d'offres d'emploi correspondant à la requête de recherche.
    """
    try:
        body = (
            {
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": ["Job", "Company", "Address", "Description"],
                    }
                }
            }
            if query
            else {"query": {"match_all": {}}}
        )
        response = await es.search(index="offres_emploi4", body=body)
        return [
            JobOffer(**hit["_source"], id=hit["_id"])
            for hit in response["hits"]["hits"]
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
