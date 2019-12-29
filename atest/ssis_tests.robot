*** Settings ***

Resource            resources/common.robot
Default Tags        SQL Server      SSIS

Test Setup          Connect to Database     SSISDB      ${ssis_connection_string}
Test Teardown       Disconnect from Databases

*** Test Cases ***
Play with the SSIS Catalog
    connect to ssis catalog     ${ssis_connection_string}
    ${catalog_properties}=      get SSIS catalog properties
    log                         ${catalog_properties}
    ${catalog_folders}=         list ssis folders
    log                         ${catalog_folders}
    should contain              ${catalog_folders}      AdventureWorksSSIS
    ${catalog_projects}=        list all SSIS projects
    log                         ${catalog_projects}
    should contain              ${catalog_projects}     ETL
    ${project_exists}=          ssis project exists     ETL
    should be true              ${project_exists}
    ${project_exists}=          ssis project exists     ETL     AdventureWorksSSIS
    should be true              ${project_exists}

Execute a package
    ${rc}=                          execute ssis package        \\SSISDB\\AdventureWorksSSIS\\ETL\\CopyFactSales.dtsx
    should be equal as integers     ${rc}                       0