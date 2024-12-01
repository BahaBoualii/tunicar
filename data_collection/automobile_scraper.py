import aiohttp
import asyncio
from bs4 import BeautifulSoup
import pandas as pd

# Base URL for automobile listings
BASE_URL = 'https://www.automobile.tn/fr/occasion/'

# List to store the scraped data
car_data = []

# List of manufacturers for identification
manufacturers = [
    'Alfa Romeo', 'Audi', 'BAIC', 'BMW', 'BYD', 'Chery', 'Chevrolet', 'Citroën', 'Cupra',
    'Dacia', 'DFSK', 'Dodge', 'Dongfeng', 'DS', 'Faw', 'Fiat', 'Ford', 'Foton', 'GAC', 'Geely',
    'Great Wall', 'Haval', 'Honda', 'Hummer', 'Hyundai', 'Infiniti', 'Isuzu', 'Iveco', 'Jaguar',
    'Jeep', 'KIA', 'Lada', 'Lancia', 'Land Rover', 'Mahindra', 'Maserati', 'Mazda', 'Mercedes',
    'MG', 'Mini', 'Mitsubishi', 'Nissan', 'Opel', 'Peugeot', 'Piaggio', 'Porsche', 'Renault', 'Seat',
    'Skoda', 'Smart', 'Ssangyong', 'Suzuki', 'TATA', 'Toyota', 'Volkswagen', 'Volvo', 'Wallyscar'
]


async def fetch_html(url, session):
    try:
        async with session.get(url, timeout=30) as response:
            response.raise_for_status()
            return await response.text()
    except Exception as e:
        print(f"Failed to fetch {url}: {e}")
        return None


async def scrape_listing_page(url, session):
    html = await fetch_html(url, session)
    if not html:
        return None

    soup = BeautifulSoup(html, 'html.parser')
    car_details = []

    try:
        title = soup.select_one('#content_container > div.occasion-details-v2 > h1').text.strip()
        car_details.append(title)

        price = soup.select_one('.price-box .price').text.strip()
        car_details.append(price)

        manufacturer = next((m for m in manufacturers if m.lower() in title.lower()), 'Other')
        car_details.append(manufacturer)
        
        category = soup.select_one('.main-specs li:nth-of-type(4) .spec-value').text.strip()
        car_details.append(category)

        fuel_type = soup.select_one('.main-specs li:nth-of-type(3) .spec-value').text.strip()
        car_details.append(fuel_type)
        
        fiscal_horsepower = soup.select_one('.main-specs li:nth-of-type(6) .spec-value').text.strip()
        car_details.append(fiscal_horsepower)

        transmission = soup.select_one('.main-specs li:nth-of-type(5) .spec-value').text.strip()
        car_details.append(transmission)

        mileage = soup.select_one('.main-specs li:nth-of-type(1) .spec-value').text.strip()
        car_details.append(mileage)

        year = soup.select_one('.main-specs li:nth-of-type(2) .spec-value').text.strip()
        car_details.append(year)

        insertion_date = soup.select_one('.main-specs li:nth-of-type(8) .spec-value').text.strip()
        car_details.append(insertion_date)

    except Exception as e:
        print(f"Error parsing details for {url}: {e}")
        return None

    return car_details


async def scrape_main_pages(session, pages=20):
    car_urls = []

    for page_num in range(1, pages + 1):
        url = f"{BASE_URL}{page_num}"
        print(f"Fetching main page: {url}")
        html = await fetch_html(url, session)

        if not html:
            continue

        soup = BeautifulSoup(html, 'html.parser')
        articles = soup.find_all('div', {'data-key': True})

        for article in articles:
            href = article.find('a', {'class': 'occasion-link-overlay'})['href']
            full_url = f"https://www.automobile.tn{href}"
            car_urls.append(full_url)

    return car_urls


async def main():
    async with aiohttp.ClientSession() as session:
        #Scrape all car listing URLs from the main pages
        car_urls = await scrape_main_pages(session, pages=175) 
        print(f"Found {len(car_urls)} car URLs.")

        #Scrape details from individual car pages
        tasks = [scrape_listing_page(url, session) for url in car_urls]
        car_details = await asyncio.gather(*tasks)

    # Filter out None values in case of errors
    global car_data
    car_data = [details for details in car_details if details is not None]

    # Convert to a DataFrame and save to CSV
    columns = ['Title', 'Price', 'Manufacturer', 'Category', 'Fuel Type', 'Fiscal Horsepower', 'Transmission', 'Mileage', 'Year', 'Insertion Date']
    df = pd.DataFrame(car_data, columns=columns)

    output_file = './data/automobile_data.csv'
    df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"Scraped data saved to {output_file}")


if __name__ == "__main__":
    asyncio.run(main())
