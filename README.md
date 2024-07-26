# eCommerce Sales Data Project

## 1. Project Overview

This project aims to manage and analyze eCommerce sales data using Microsoft Fabric's Medallion Architecture within a Data Lakehouse. The project involves ingesting, transforming, and archiving data using Delta tables and notebooks. The end goal is to ensure the data is up-to-date and stored efficiently, with the ability to handle changes in dimensions over time.

## 2. Data Sources

### Excel File

The primary data source is an Excel file containing two tabs:

- **Tab 1: Sales** – Contains sales data.
- **Tab 2: Return** – Contains return data.

## 3. Medallion Architecture

The project follows the Medallion Architecture, which consists of multiple layers to manage data in a structured way:

- **Bronze Layer**: Raw data ingestion.
- **Silver Layer**: Data transformation and enrichment.
- **Gold Layer**: Aggregated and highly curated data for analysis.

### Tables Created

#### Bronze Layer
- **Bronze_Sales**: Raw sales data from the Excel file.

#### Gold Layer
- **Gold_Order_return**: Processed and curated return data.
- **Gold_Orderpriority**: Order priority information.
- **Gold_Customer**: Customer details.
- **Gold_Product**: Product details.
- **Gold_ShipMode**: Shipping mode details.
- **Gold_Date**: Date dimension.
- **Gold_Fact_sales**: Aggregated sales data, incorporating various dimensions.

## 4. Data Storage and Management

### File Organization

- **Current Folder**: This is the active folder where new files are initially stored.
- **Archive Folder**: This folder stores files that have been processed and updated.

### Data Flow Process

1. **Data Ingestion**: Files are initially stored in the Current folder.
2. **Data Processing**: The `load_run` notebook checks for new files in the Current folder.
3. **Notebook Execution**: If files are present, the notebook runs all processing notebooks to update the Delta tables.
4. **File Archival**: After processing, the file is moved to the Archive folder.

## 5. Automation

### Notebook Execution

- **`load_run` Notebook**: Utilizes the `mssparkutils` API to:
  - Check for the existence of files in the Current folder.
  - Trigger the execution of all relevant notebooks to update the data.
  - Transfer processed files to the Archive folder.

### Data Pipeline

- A data pipeline is configured to use the Notebook Run functionality.
- **Source**: The pipeline is attached to the `load_run` notebook to automate the data processing and archival process.

## 6. Notebook Details

Each notebook is responsible for processing different aspects of the data:

- **Bronze_Sales Notebook**: Handles raw data ingestion and stores it in the `Bronze_Sales` Delta table.
- **Gold Layer Notebooks**: Each notebook (e.g., `Gold_Order_return`, `Gold_Orderpriority`, etc.) transforms and populates the corresponding Gold Delta tables.

## 7. Handling Dimension Changes

- **Dynamic Updates**: The design of each notebook accommodates changes in dimensions, ensuring that updates are reflected in the Gold tables.
- **Scalability**: As new dimensions or attributes are added, the notebooks can be updated to handle these changes seamlessly.

## 8. Summary

This project effectively utilizes Microsoft Fabric's Data Lakehouse capabilities to manage eCommerce sales data. By leveraging the Medallion Architecture and automating the data processing workflow, the project ensures data is always up-to-date and archived efficiently. The integration of notebooks and pipelines provides a robust solution for ongoing data management and analysis.

## 9. Future Enhancements

- **Additional Data Sources**: Integration with other data sources for a more comprehensive analysis.
- **Advanced Analytics**: Implementing machine learning models for predictive analytics on sales and returns.
- **Dashboarding**: Creating interactive dashboards for real-time data visualization and insights.



