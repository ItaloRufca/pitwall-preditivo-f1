import logging
import sys
from dotenv import load_dotenv
from src.ingestion.ingest_bronze import run_ingestion

def setup_logging():
    """Configura o logging básico para a aplicação."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

if __name__ == "__main__":
    load_dotenv()
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Iniciando processo de ingestão...")
        run_ingestion()
        logger.info("Processo de ingestão finalizado com sucesso.")
    except Exception as e:
        logger.error(f"Erro fatal durante a execução: {e}", exc_info=True)
        sys.exit(1)
