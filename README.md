# Instructure Canvas Data 2 Import Script

This project contains a Django management command to automate the import of TSV files generated by **Instructure Canvas Data 2** CLI into a PostgreSQL database. The script processes CSV files, detects those created recently, and performs an upsert operation to avoid data duplication.

## Features
- **Automated Data Import**: Imports Canvas Data 2 TSV files directly into PostgreSQL tables.  
- **Upsert Support**: Handles conflicts by updating existing records while inserting new ones.  
- **Table Support**: Supports importing data into six key tables:  
  - `users`  
  - `courses`  
  - `enrollments`  
  - `pseudonyms`  
  - `enrollment_terms`  
  - `course_sections`  
- **Environment Variable Configuration**: Database connection details are securely managed using environment variables.  

---

## How to Use

1. Clone the Repository  
```bash
git clone https://github.com/rmazroey/Instructure-Canvas-Data-2-.git
cd Instructure-Canvas-Data-2-

2. Install Dependencies
Ensure pipenv is installed and run
```bash
pipenv install

3. Set Up Environment Variables
Create a .env file in the root directory with the following structure:

POSTGRES_HOST=localhost
POSTGRES_USER=yourusername
POSTGRES_PASSWORD=yourpassword
POSTGRES_NAME=yourdatabase

4. Run the Import Command
pipenv run python manage.py import_canvasdata2 --table users
pipenv run python manage.py import_canvasdata2 --table courses
pipenv run python manage.py import_canvasdata2 --table enrollments
pipenv run python manage.py import_canvasdata2 --table pseudonyms
pipenv run python manage.py import_canvasdata2 --table enrollment_terms
pipenv run python manage.py import_canvasdata2 --table course_sections

