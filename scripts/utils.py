def split_into_batches(logger, items:list, batch_size:int=50) -> list:
    """
    Splits a list of URIs into batches of a specified size

    Args:
        logger: Logger instance.
        items (list): The list to split
        batch_size (int): The size of each batch

    Returns:
        list: A list of batches, where each batch is a list of items
    """
    logger.info(f"Splitting {len(items)} items into batches of size {batch_size}")
    return [items[i:i + batch_size] for i in range(0, len(items), batch_size)] if items else []