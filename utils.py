from tabulate import tabulate

def format_locks(locks):
    if not locks:
        return "No locks collected."
    
    headers = [
        "Object Type",
        "Object Schema",
        "Object Name",
        "Lock Type",
        "Lock Status"
    ]

    return tabulate(locks, headers=headers, tablefmt="grid")
