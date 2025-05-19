def list_to_tuple_string(data):
    """
    Escapa comas y comillas en texto usando comillas dobles.
    No envuelve valores NULL ('\\N') en comillas.
    """
    escaped = []
    for item in data:
        # Dejar el marcador NULL sin comillas
        if item == '\\N':
            escaped.append(item)
        elif ',' in item or '"' in item:
            safe = item.replace('"', '""')
            escaped.append(f'"{safe}"')
        else:
            escaped.append(item)
    return ','.join(escaped)