# utils/logger.py
import logging
import sys

def get_logger(name: str = "multi_model_demo_dag") -> logging.Logger:
    """
    Trả về logger đã được config đẹp, dùng chung cho toàn bộ DAG và tasks
    """
    logger = logging.getLogger(name)
    
    # Nếu logger đã được config rồi thì trả về luôn (tránh duplicate handler)
    if logger.handlers:
        return logger
    
    logger.setLevel(logging.INFO)
    
    # Handler in ra stdout (để kubectl logs thấy đẹp)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    
    # Format đẹp như bạn thích
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    ch.setFormatter(formatter)
    
    logger.addHandler(ch)
    
    # Không propagate lên root logger (tránh in 2 lần)
    logger.propagate = False
    
    return logger