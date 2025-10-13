import random
from datetime import datetime, timedelta

# -------------------- Static Datasets -------------------- #
ships = [
    {"shipId": "SHIP_001", "imoNumber": "1234567", "name": "EverGreen", "flag": "Panama", "capacityTEU": 20000},
    {"shipId": "SHIP_002", "imoNumber": "7654321", "name": "Maersk", "flag": "Denmark", "capacityTEU": 18000},
    {"shipId": "SHIP_003", "imoNumber": "2468135", "name": "COSCO", "flag": "China", "capacityTEU": 21000},
    {"shipId": "SHIP_004", "imoNumber": "1357246", "name": "MSC", "flag": "Switzerland", "capacityTEU": 19500},
    {"shipId": "SHIP_005", "imoNumber": "1122334", "name": "Hapag-Lloyd", "flag": "Germany", "capacityTEU": 17000},
    {"shipId": "SHIP_006", "imoNumber": "2233445", "name": "ONE", "flag": "Japan", "capacityTEU": 14500},
    {"shipId": "SHIP_007", "imoNumber": "3344556", "name": "CMA CGM", "flag": "France", "capacityTEU": 22000},
    {"shipId": "SHIP_008", "imoNumber": "4455667", "name": "Yang Ming", "flag": "Taiwan", "capacityTEU": 14000},
    {"shipId": "SHIP_009", "imoNumber": "5566778", "name": "ZIM", "flag": "Israel", "capacityTEU": 12500},
    {"shipId": "SHIP_010", "imoNumber": "6677889", "name": "OOCL", "flag": "Hong Kong", "capacityTEU": 21000},  
    {"shipId": "SHIP_011", "imoNumber": "7788990", "name": "APL", "flag": "Singapore", "capacityTEU": 15000},
    {"shipId": "SHIP_012", "imoNumber": "8899001", "name": "K Line", "flag": "Japan", "capacityTEU": 13500},
    {"shipId": "SHIP_013", "imoNumber": "9900112", "name": "HMM", "flag": "South Korea", "capacityTEU": 23964},
    {"shipId": "SHIP_014", "imoNumber": "1011123", "name": "Wan Hai", "flag": "Taiwan", "capacityTEU": 11500},
    {"shipId": "SHIP_015", "imoNumber": "1213141", "name": "Seaspan", "flag": "Canada", "capacityTEU": 10500},   
    {"shipId": "SHIP_016", "imoNumber": "1415161", "name": "Ever Ace", "flag": "Panama", "capacityTEU": 23992},
    {"shipId": "SHIP_017", "imoNumber": "1516171", "name": "Madrid Maersk", "flag": "Denmark", "capacityTEU": 20568},
    {"shipId": "SHIP_018", "imoNumber": "1617181", "name": "MSC Gülsün", "flag": "Liberia", "capacityTEU": 23756},
    {"shipId": "SHIP_019", "imoNumber": "1718191", "name": "Al Zubara", "flag": "Qatar", "capacityTEU": 18800},
    {"shipId": "SHIP_020", "imoNumber": "1819202", "name": "Triton", "flag": "UK", "capacityTEU": 14600},   
    {"shipId": "SHIP_021", "imoNumber": "1920212", "name": "MOL Triumph", "flag": "Japan", "capacityTEU": 20170},
    {"shipId": "SHIP_022", "imoNumber": "2021222", "name": "CMA CGM Jacques Saadé", "flag": "France", "capacityTEU": 23000},
    {"shipId": "SHIP_023", "imoNumber": "2122232", "name": "Xin Shanghai", "flag": "China", "capacityTEU": 15000},
    {"shipId": "SHIP_024", "imoNumber": "2223242", "name": "Italia Maranello", "flag": "Italy", "capacityTEU": 13000},
    {"shipId": "SHIP_025", "imoNumber": "2324252", "name": "Sapura Diamante", "flag": "Malaysia", "capacityTEU": 12000},    
    {"shipId": "SHIP_026", "imoNumber": "2425262", "name": "Victoria Harbour", "flag": "Hong Kong", "capacityTEU": 10000},
    {"shipId": "SHIP_027", "imoNumber": "2526272", "name": "Hamburg Express", "flag": "Germany", "capacityTEU": 16000},
    {"shipId": "SHIP_028", "imoNumber": "2627282", "name": "Tokyo Bay", "flag": "Japan", "capacityTEU": 14000},
    {"shipId": "SHIP_029", "imoNumber": "2728292", "name": "Seoul Star", "flag": "South Korea", "capacityTEU": 15500},
    {"shipId": "SHIP_030", "imoNumber": "2829302", "name": "Dubai Crown", "flag": "UAE", "capacityTEU": 17000},
    {"shipId": "SHIP_031", "imoNumber": "2920312", "name": "New York Spirit", "flag": "USA", "capacityTEU": 16500}
]


ship_event_types = ["ARRIVAL", "DEPARTURE", "DELAY"]
ship_statuses = ["DOCKED", "LOADING", "UNLOADING", "DEPARTED"]
berths = [f"BERTH_{i:02d}" for i in range(1, 11)]

sizes = ["20ft", "40ft"]
container_event_types = ["LOAD", "UNLOAD", "TRANSFER"]
container_statuses = ["IN_PORT", "ON_SHIP", "DELIVERED"]
yards = [f"YARD_{chr(row)}{col}" for row in range(65, 70) for col in range(1, 6)]

product_categories = {
    "Food": ["Rice", "Wheat", "Bananas", "Apples", "Oranges"],
    "Automobile": ["Car", "Truck", "Motorbike", "SpareParts"],
    "Electronics": ["TV", "Laptop", "Smartphone", "Tablet", "Router"],
    "Furniture": ["Chair", "Table", "Sofa", "Bed", "Desk"],
    "Clothing": ["Shirt", "Jeans", "Shoes", "Jacket"]
}
product_statuses = ["IN_CONTAINER", "UNLOADED", "DELIVERED"]

truck_statuses = ["LOADED", "ON_ROUTE", "DELIVERED"]
staff_roles = ["Port Operator", "Crane Operator", "Customs", "Driver"]
staff_event_types = ["START_SHIFT", "END_SHIFT", "BREAK"]
staff_shifts = ["Morning", "Evening", "Night"]
inspection_results = ["CLEARED", "FLAGGED", "HELD"]
warehouses = [f"WAREHOUSE_{i}" for i in range(1, 11)]

# -------------------- Helper Functions -------------------- #

def gen_container_id():
    return "CONT_" + "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", k=6))

def gen_seal():
    return "SEAL_" + str(random.randint(10000, 99999))

def gen_product_id():
    return "PROD_" + "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", k=8))

def gen_staff_id():
    return "STF_" + str(random.randint(1000, 9999))

def gen_truck_id():
    return "TRUCK_" + str(random.randint(1, 50)).zfill(3)

def gen_driver_id():
    return "DRV_" + str(random.randint(1, 50)).zfill(3)

def gen_inspection_id():
    return "INSP_" + str(random.randint(1, 999)).zfill(3)

def random_past_datetime():
    """Generate random datetime between 2011 and now"""
    start = datetime(2011, 1, 1)
    end = datetime.utcnow()
    delta = end - start
    random_days = random.randint(0, delta.days)
    random_seconds = random.randint(0, 86400)
    return (start + timedelta(days=random_days, seconds=random_seconds)).isoformat() + "Z"

names = [
    "John Doe",
    "Jane Smith",
    "Ali Khan",
    "Maria Gomez",
    "Robert Lee",
    "Emma Johnson",
    "Liam Brown",
    "Noah Williams",
    "Olivia Davis",
    "Ava Martinez",
    "Sophia Rodriguez",
    "Isabella Garcia",
    "Ethan Wilson",
    "Mason Taylor",
    "Logan Anderson",
    "Amelia Thomas",
    "James Moore",
    "William Jackson",
    "Benjamin White",
    "Lucas Harris",
    "Henry Clark",
    "Alexander Lewis",
    "Daniel Young",
    "Chloe Hall",
    "Grace Allen"
]
