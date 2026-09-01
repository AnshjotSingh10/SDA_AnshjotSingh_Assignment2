"""
Faker-generated product catalog and page catalog, shared by both producers
so that page views, searches, clicks, carts, and purchases reference the
same consistent set of products/pages.
"""
import random
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

CATEGORIES = [
    "Electronics", "Home & Kitchen", "Fashion", "Beauty", "Sports",
    "Books", "Toys", "Groceries", "Automotive", "Furniture",
]

PAGE_TYPES = ["home", "category", "product"]


def _generate_products(n=60):
    products = []
    for i in range(n):
        category = random.choice(CATEGORIES)
        products.append({
            "product_id": f"P{i+1:04d}",
            "product_name": fake.unique.catch_phrase(),
            "category": category,
            "unit_price": round(random.uniform(5, 500), 2),
        })
    return products


PRODUCT_CATALOG = _generate_products(60)


def random_product():
    return random.choice(PRODUCT_CATALOG)


def random_page():
    page_type = random.choice(PAGE_TYPES)
    if page_type == "product":
        product = random_product()
        return {"page_type": "product", "page_ref": product["product_id"], "product": product}
    elif page_type == "category":
        return {"page_type": "category", "page_ref": random.choice(CATEGORIES), "product": None}
    else:
        return {"page_type": "home", "page_ref": "home", "product": None}


def random_search_term():
    candidates = [p["product_name"].split()[0] for p in PRODUCT_CATALOG] + CATEGORIES
    return random.choice(candidates)


def new_user():
    return {
        "user_id": fake.uuid4(),
        "name": fake.name(),
        "email": fake.email(),
        "country": fake.country(),
    }
