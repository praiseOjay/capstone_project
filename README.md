# ETL Capstone Project

A comprehensive Python-based ETL (Extract, Transform, Load) pipeline with an integrated data visualisation dashboard, focusing on fitness statistics analysis with professional development practices.

## Project Overview

This capstone project demonstrates a complete data engineering solution that extracts fitness data, transforms it through comprehensive cleaning and enrichment processes, loads it into structured formats, and provides interactive visualisations through a Streamlit dashboard. The project emphasises code quality, testing, and industry-standard development practices.

## 🏗️ Architecture

The project follows a modular ETL architecture with the following components:

- **Extract**: Data extraction from CSV sources with comprehensive logging
- **Transform**: Multi-stage data cleaning, validation, and enrichment
- **Load**: Flexible data output to CSV and Parquet formats
- **Visualisation**: Interactive Streamlit dashboard with multiple views
- **Configuration**: Environment-based configuration management
- **Testing**: Comprehensive test suite with unit, integration, and E2E tests

## 📁 Project Structure
capstone_project/  
├── config/ # Configuration management   
│ ├── db_config.py # Database configuration utilities   
│ └── env_config.py # Environment setup and management   
├── data/ # Data storage directory   
│ ├── raw/ # Raw, unprocessed data   
│ ├── processed/ # Cleaned and transformed data   
│ └── external/ # External data sources  
├── scripts/ # Automation and execution scripts   
│ ├── run_app.py # Main application runner    
│ └── run_etl.py # ETL pipeline runner   
├── src/ # Source code   
│ ├── etl/ # ETL pipeline components   
│ │ ├── extract/ # Data extraction modules  
│ │ ├── transform/ # Data transformation and cleaning   
│ │ └── load/ # Data loading utilities   
│ ├── streamlit/ # Dashboard application    
│ │ ├── app.py # Main Streamlit app  
│ │ └── pages/ # Dashboard pages   
│ └── utils/ # Utility functions  
├── tests/ # Comprehensive test suite   
│ ├── unit_tests/ # Unit tests   
│ ├── integration_tests/ # Integration tests   
│ ├── component_tests/ # Component tests  
│ └── e2e_tests/ # End-to-end tests  
├── .coveragerc # Coverage configuration  
├── .flake8 # Python linting configuration  
├── .sqlfluff # SQL linting configuration  
├── pyproject.toml # Project metadata and build config  
└── requirements.txt # Project dependencies  

## 🚀 Features

### ETL Pipeline
- **Robust Data Extraction**: CSV file processing with error handling and performance monitoring
- **Comprehensive Data Cleaning**: Missing value handling, duplicate removal, data type conversion
- **Data Enrichment**: BMI recalculation, participant metrics, weekly aggregations
- **Flexible Output**: Support for CSV and Parquet formats with compression options

### Data Transformations
- Date standardisation and formatting
- Categorical value standardisation
- Missing value imputation strategies
- Duplicate detection and removal
- Calculated field generation (BMI, weekly metrics)

### Interactive Dashboard
- **Dashboard Overview**: Key metrics and summary statistics
- **Seasonal Patterns**: Activity trends by season and time periods

### Quality Assurance
- **Comprehensive Testing**: Unit, integration, component, and E2E tests
- **Code Quality**: Automated linting with flake8 and sqlfluff
- **Coverage Reporting**: Test coverage tracking and reporting
- **Logging**: Structured logging throughout the pipeline

## 🛠️ Getting Started

### Prerequisites

- Python 3.6 or higher
- pip package manager
- Virtual environment (recommended)

### Installation

1. **Clone the repository:**
```bash
git clone https://github.com/praiseOjay/capstone_project.git
cd capstone_project
```
2. **Create and activate a virtual environment:**
   ### On Windows
   python -m venv venv  
   venv\Scripts\activate  

   ### On macOS/Linux
   python3 -m venv venv   
   source venv/bin/activate   

3. **Install dependencies:**  
    pip install -r requirements.txt  

4. **Set up environment configuration:**
   #### Copy and configure environment files
   cp .env.dev .env  # For development   
   Edit .env with your specific configuration 

5. **Get the raw dataset from:**   
   https://www.kaggle.com/datasets/jijagallery/fitlife-health-and-fitness-tracking-dataset  

## 📊 Usage
### Running the Complete Application
#### Run both the ETL pipeline and the Streamlit dashboard  
run_app (test, dev)   

### Running Components Separately 
#### Run only the ETL pipeline  
run_etl_only (test, dev)    

#### Run only the Streamlit dashboard   
run_streamlit_only (test, dev)     

### Dashboard Access
After starting the Streamlit application, access the dashboard at: 

http://localhost:8501  
The dashboard includes:  

- Dashboard: Overview of key fitness metrics and distributions
- Fitness Progression: Individual participant progress tracking
- Seasonal Patterns: Activity trends and seasonal analysis   

## 🧪 Testing
The project includes a comprehensive test suite covering all components:   

### Running Tests   
#### Run specific test categories    
run_tests unit          # Unit tests  
run_tests integration   # Integration tests   
run_tests component     # Component tests   
run_tests all          # End-to-end tests   

### Test Coverage   
The project maintains high test coverage across:    

- ETL pipeline components (extract, transform, load)  
- Utility functions (file operations, logging)  
- Configuration management  
- Data processing functions  
## 🔧 Configuration   
### Environment Management   
The project supports multiple environments through configuration files:   

.env.dev - Development environment    
.env.test - Testing environment     

### Code Quality Standards
- flake8: Python code linting (configured in .flake8)   
- sqlfluff: SQL formatting and style checking (configured in .sqlfluff)   
- coverage: Test coverage tracking (configured in .coveragerc)    
  
###  Quality Checks
#### Check Python code style   
flake8 src/ tests/    

### Check SQL formatting   
sqlfluff lint  

### Run all quality checks  
flake8 src/ tests/ && sqlfluff lint    

## 📈 Data Pipeline
### Extract Phase
- Reads fitness statistics from CSV files
- Validates data integrity and structure
- Logs extraction metrics and performance
### Transform Phase
- Data Cleaning: Handles missing values, removes duplicates
- Data Standardisation: Formats dates, standardises categorical values
- Data Enrichment: Calculates BMI, adds participant metrics
- Data Filtering: Prepares visualisation-ready datasets
### Load Phase
- Saves processed data to CSV and Parquet formats
- Supports configurable output directories
- Includes data validation and success logging
## 🤝 Contributing
1. Fork the repository
2. Create a feature branch (git checkout -b feature/your-feature)
3. Write tests for new functionality
4. Ensure all tests pass (pytest tests/)
5. Run code quality checks (flake8 src/ tests/)
6. Commit your changes (git commit -m 'Add some feature')
7. Push to the branch (git push origin feature/your-feature)
8. Open a Pull Request
### Contribution Guidelines
- Follow PEP 8 style guidelines
- Write comprehensive tests for new features
- Update documentation as needed
- Ensure test coverage remains high
- Include type hints where appropriate
## 📚 Documentation
The project includes comprehensive documentation:

- Code Documentation: Inline docstrings and comments
- Test Documentation: Test descriptions and fixtures
- Configuration Documentation: Environment setup guides

## 🔍 Monitoring and Logging
The project includes structured logging throughout:

- ETL Logging: Pipeline execution, data quality metrics
- Performance Logging: Execution times, data volumes
- Error Logging: Exception handling and error reporting
- Success Logging: Completion confirmations and summaries
  
## 📄 License
This project currently has no license specified. Please contact the repository owner for usage rights.  

## 👨‍💻 Author
Praise Ojerinola (praiseOjay)

- GitHub: [@praiseOjay](https://github.com/praiseOjay)
- Email: ojerinolapraise@gmail.com

















