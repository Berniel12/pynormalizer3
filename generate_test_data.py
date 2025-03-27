import os
import json
import random
from datetime import datetime, timedelta
from tendertrail_integration import TenderTrailIntegration

# Configuration for test data generation
SOURCES = ["adb", "afd", "afdb", "aiib", "iadb", "sam_gov", "ted_eu", "ungm", "wb"]
TENDER_TYPES = ["Goods", "Works", "Services", "Consulting"]
COUNTRIES = ["United States", "France", "Germany", "United Kingdom", "Canada", "Japan", "Australia", "Brazil", "India", "China"]
CITIES = ["New York", "Paris", "Berlin", "London", "Toronto", "Tokyo", "Sydney", "Rio de Janeiro", "Mumbai", "Beijing"]
ORGANIZATIONS = [
    "Ministry of Finance", "Department of Transportation", 
    "Ministry of Education", "Department of Defense",
    "Ministry of Health", "Department of Energy",
    "National Water Authority", "Environmental Protection Agency",
    "Department of Agriculture", "Ministry of Infrastructure"
]
TENDER_TITLES = [
    "Construction of Water Treatment Plant in {city}",
    "Supply of Medical Equipment for {country} Hospitals",
    "IT Infrastructure Upgrade for {organization}",
    "Consulting Services for {country} Railway Project",
    "Road Rehabilitation in {city} Metropolitan Area",
    "Supply and Installation of Solar Panels for {organization}",
    "Environmental Impact Assessment for {city} Airport",
    "Technical Assistance for {country} Education Reform",
    "Procurement of Vehicles for {organization}",
    "Design and Construction of Bridge in {city}"
]

def generate_random_date(start_date=None, max_days_ahead=180):
    """Generate a random date."""
    if not start_date:
        start_date = datetime.now()
    random_days = random.randint(1, max_days_ahead)
    return (start_date + timedelta(days=random_days)).strftime("%Y-%m-%d")

def generate_random_description(title, organization, city, country):
    """Generate a detailed description based on the title."""
    paragraphs = [
        f"The {organization} is seeking proposals for {title.format(city=city, country=country, organization=organization)}.",
        f"This project aims to improve infrastructure in {city}, {country}, and will be implemented under the supervision of {organization}.",
        f"Eligible bidders should have relevant experience in similar projects and must comply with all regulatory requirements.",
        f"The successful bidder will be responsible for all aspects of the project, including planning, implementation, and monitoring."
    ]
    return "\n\n".join(paragraphs)

def generate_test_tender(source):
    """Generate a realistic test tender for a given source."""
    # Select random values
    country = random.choice(COUNTRIES)
    city = random.choice(CITIES)
    organization = random.choice(ORGANIZATIONS)
    tender_type = random.choice(TENDER_TYPES)
    title_template = random.choice(TENDER_TITLES)
    
    # Create publication and closing dates
    pub_date = generate_random_date(datetime.now() - timedelta(days=30), 30)
    closing_date = generate_random_date(datetime.strptime(pub_date, "%Y-%m-%d"), 90)
    
    # Generate tender value
    value = round(random.uniform(100000, 10000000), 2)
    currency = random.choice(["USD", "EUR", "GBP", "JPY", "AUD"])
    
    # Format title
    title = title_template.format(city=city, country=country, organization=organization)
    
    # Generate detailed description
    description = generate_random_description(title_template, organization, city, country)
    
    # Create a unique ID
    tender_id = f"{source.upper()}-{random.randint(1000, 9999)}-{random.randint(10, 99)}"
    
    # Create base tender data with standard fields
    tender = {
        "title": title,
        "description": description,
        "tender_type": tender_type,
        "country": country,
        "location": f"{city}, {country}",
        "organization": organization,
        "publication_date": pub_date,
        "closing_date": closing_date,
        "value": f"{value} {currency}",
        "currency": currency,
        "tender_id": tender_id,
        "source": source
    }
    
    # Add source-specific fields
    if source == "adb":
        tender["adb_number"] = f"ADB-{random.randint(10000, 99999)}"
        tender["sector"] = random.choice(["Energy", "Transport", "Water", "Urban", "Finance", "Health"])
    elif source == "wb":
        tender["wb_reference"] = f"WB-{random.randint(10000, 99999)}"
        tender["borrower"] = random.choice(["Government of " + country, organization])
    elif source == "ted_eu":
        tender["cpv_codes"] = [f"{random.randint(10000, 99999)}", f"{random.randint(10000, 99999)}"]
        tender["nuts_code"] = f"NUTS-{random.randint(1, 5)}"
    elif source == "sam_gov":
        tender["solicitation_id"] = f"SOL-{random.randint(1000, 9999)}"
        tender["agency"] = "US " + organization
    
    return tender

def generate_and_upload_test_data():
    """Generate test data for each source and upload to Supabase."""
    # Get environment variables for Supabase connection
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    
    if not supabase_url or not supabase_key:
        print("Supabase credentials not found in environment variables.")
        print("Please set SUPABASE_URL and SUPABASE_KEY environment variables.")
        return
    
    # Initialize TenderTrailIntegration
    integration = TenderTrailIntegration(supabase_url=supabase_url, supabase_key=supabase_key)
    
    # Generate test data for each source
    for source in SOURCES:
        tenders = []
        
        # Generate 5 tenders per source
        for _ in range(5):
            tender = generate_test_tender(source)
            tenders.append(tender)
        
        print(f"Generated {len(tenders)} test tenders for source: {source}")
        
        # Process the tenders
        try:
            processed_count, error_count = integration.process_json_tenders(tenders, source)
            print(f"Processed {processed_count} tenders for {source} with {error_count} errors")
        except Exception as e:
            print(f"Error processing tenders for {source}: {e}")
    
    print("Test data generation completed")

if __name__ == "__main__":
    generate_and_upload_test_data() 