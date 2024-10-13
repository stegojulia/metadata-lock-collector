from tabulate import tabulate

def format_locks(locks):
    if not locks:
        return "No locks collected."
    
    headers = [
        "Object Type",
        "Object Schema",
        "Object Name",
        "Lock Type",
        "Lock Status",
        "Owner Event ID",
        "Owner Thread ID"
    ]

    return tabulate(locks, headers=headers, tablefmt="grid")
