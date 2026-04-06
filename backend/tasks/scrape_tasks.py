from celery import shared_task


@shared_task(name="scrape_car", bind=True)
def scrape_car(self, car_number: str, run_id: str, **kwargs):
    """
    Celery worker task — processes ONE car

    Args:
        car_number: vehicle number
        run_id: scrape run id
        kwargs: optional fields (phone, cust_name, etc.)
    """

    print(f"🚗 Processing car: {car_number} | run: {run_id}")

    try:
        # 🔹 Your scraping logic here
        # Example:
        # result = scrape_insurance(car_number)

        print(f"✅ Done: {car_number}")

        return {
            "car_number": car_number,
            "status": "success",
        }

    except Exception as e:
        print(f"❌ Error for {car_number}: {str(e)}")
        raise e