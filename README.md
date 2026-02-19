# Overview

Async Python script to bulk rename sites in Cisco SD-WAN vManage using the network-hierarchy API with built-in rate limiting, retry logic, and error handling.


# Prerequisites

System Requirements

    Python 3.8+
    Network access to SD-WAN vManage
    Valid vManage credentials with site management permissions


Python Dependencies

pip install aiohttp pandas python-dotenv openpyxl



# Installation

Step 1: Install Dependencies

pip install aiohttp pandas python-dotenv openpyxl


Step 2: Create .env File


#SD-WAN vManage Configuration

    VMANAGE_URL=https://your-vmanage-ip-or-hostname
    VMANAGE_USERNAME=your_username
    VMANAGE_PASSWORD=your_password

#Async Configuration (IMPORTANT: Keep at 1 to avoid deadlocks)
MAX_CONCURRENT_REQUESTS=1

#Spreadsheet Configuration
SPREADSHEET_PATH=site_mapping.xlsx

#Test Mode (set to true to test with one site first)
TEST_MODE=true


Step 3: Prepare Spreadsheet

Create an Excel or CSV file with two columns:
Important: 

    Column A = Current site name (as it appears in vManage)
    Column B = Desired new site name
    No headers required (script uses first two columns)



# Usage

Test Mode (Recommended First)



#Set TEST_MODE=true in .env



This will:

    Fetch all sites\
    Find the first matching site
    Update only that one site
    Show success/failure


Production Mode



#Set TEST_MODE=false in .env



This will:

    Update all matching sites from spreadsheet
    Process sequentially (to avoid deadlocks)
    Show progress every 10 sites
    Generate summary report



# API Endpoints Used

1. Authentication

POST /j_security_check
GET /dataservice/client/token

    Purpose: Obtain JSESSIONID and X-XSRF-TOKEN
    Auth: Form-based authentication


2. Get Network Hierarchy

GET /dataservice/v1/network-hierarchy

    Purpose: Retrieve all sites with hierarchy information
    Response: List of sites with:
        uuid: Site UUID
        name: Site name
        data.hierarchyId.siteId: Numeric site ID
        data.parentUuid: Parent site UUID
        data.label: Site label
        data.location: Site location


3. Update Site

PUT /dataservice/v1/network-hierarchy/{uuid}

    Purpose: Rename a site
 

Script Workflow

Step 1: Configuration Loading

    Load credentials from .env file
    Load site name mappings from Excel/CSV
    Validate all required variables present


Step 2: Authentication

    Login via form-based j_security_check
    Obtain JSESSIONID cookie
    Get X-XSRF-TOKEN\
    Store credentials for subsequent requests


Step 3: Fetch Sites

    GET /dataservice/v1/network-hierarchy\
    Parse response (handles list or dict format)
    Extract: uuid, name, siteId, parentUuid, label, location
    Include Global site (needed for parent lookups)


Step 4: Match & Filter

    Compare vManage sites against spreadsheet mappings
    Skip sites where name unchanged
    Skip sites missing required fields (uuid, parentUuid)
    Use Global UUID for sites without parentUuid


Step 5: Update Sites

Test Mode: Update first matching site only
Production Mode: Update all matching sites

For each site:

    Extract parent UUID from site data
    Build PUT payload with all required fields
    Send update request\
    Handle errors with retry logic (3 attempts)
    Apply rate limiting (80 calls/minute)
    Add 1 second delay between updates


Step 6: Progress & Summary

    Log each update (success/failure)
    Show progress every 10 sites
    Display final summary with statistics


# Performance

API Call Count

Total API Calls = 2 (auth) + 1 (get hierarchy) + N (updates)

Example:

    50 sites to rename = 2 + 1 + 50 = 53 API calls\
    100 sites to rename = 2 + 1 + 100 = 103 API calls

Execution Time
Formula: ~2 seconds per site (1s API + 1s delay)


Logging

Log Files

    Console: Real-time progress and errors
    File: sdwan_site_rename.log - Detailed execution log

Log Levels

    INFO: Progress, success messages
    WARNING: Retries, rate limiting
    ERROR: Failures, validation errors


# Security Considerations

Credentials

    ✅ Stored in .env file (not in code)
    ✅ .env excluded from version control

SSL/TLS

    ⚠️ SSL verification disabled (for self-signed certificates)
    ✅ Only use on trusted networks


Permissions Required

    Read: Network hierarchy
    Write: Network hierarchy updates
    Session: API access enabled

# Files

Input Files

    .env - Configuration and credentials
    site_mapping.xlsx or .csv - Site name mappings


Output Files

    sdwan_site_rename.log - Detailed execution log


Script Files

    sdwan-name-update.py - Main script
