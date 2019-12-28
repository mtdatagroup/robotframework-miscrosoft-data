*** Settings ***

Resource            resources/common.robot
Library             OperatingSystem

Test Setup          Connect to Database     AdventureWorks      ${connection_string}
Test Teardown       Disconnect from Databases
Default Tags        Database        SQL Server      windows

*** Variables ***
${FIXTURE}                      ${TESTDATA_DIRECTORY}/test.csv

*** Test Cases ***
Interact with Connection
    ${connection_name}=         current connection name
    log                         ${connection_name}
    ${connections}=             list connections
    log                         ${connections}
    Connect to Database         Bob         ${connection_string}
    ${connections}=             list connections
    log                         ${connections}
    switch connection           Bob
    ${connection_name}=         current connection name
    log                         ${connection_name}

Play with SQL Server
    ${df}=                      read query              SELECT TOP 10 * FROM dbo.DimCustomer
    Log                         ${df}
    ${dict}=                    read query              SELECT TOP 10 * FROM dbo.DimCustomer
    Log                         ${dict}
    ${rec_count}=               read scalar             SELECT COUNT(*) FROM dbo.DimCustomer
    ${rec_count2}=              table row count         dbo     DimCustomer
    should be equal as integers                         ${rec_count}
    ...                                                 ${rec_count2}
    ${rec_count3}=              query row count         SELECT * FROM dbo.DimCustomer
    should be equal as integers                         ${rec_count}
    ...                                                 ${rec_count3}
    ${non_scalar}=              read scalar             SELECT * FROM dbo.DimCustomer
    Log                         ${non_scalar}
    ${tables}=                  list tables             dbo
    log                         ${tables}
    ${exists}=                  table exists            dbo     DimCustomer
    should be true              ${exists}

Logon to SQL Server with trusted connection
    Connect to Database         trusted_conn            ${trusted_connection_string}
    Use Pandas                  ${TRUE}
    ${dict}=                    read query              SELECT TOP 10 * FROM dbo.DimCustomer
    Log                         ${dict}

Load data fixtures
    ${fixture_contents}=        Get File                ${FIXTURE}
    Log                         ${fixture_contents}
    truncate table              dbo                     NameAgeTable
    ${row_count}=               table row count         dbo                 NameAgeTable
    should be equal as integers                         ${row_count}        0
    ${row_count}=               load table with CSV     ${FIXTURE}
    ...                                                 dbo
    ...                                                 NameAgeTable
    should not be equal as integers                     ${row_count}        0
    ${records}=                 read_query              SELECT * FROM dbo.NameAgeTable
    Log                         ${records}
    ${records2}=                read table              dbo         NameAgeTable
    log                         ${records2}
    ${metadata}=                get table metadata      dbo         NameAgeTable
    log                         ${metadata}

Play with Procedures and Functions
    ${functions}=               list functions
    log                         ${functions}
    ${procs}=                   list procedures
    log                         ${procs}
    ${result}=                  read query              select dbo.udfTwoDigitZeroFill(1)
    log                         ${result}
    ${result2}=                 execute procedure       SelectAllCustomers
    log                         ${result2}
    ${params}=                  create list             1
    ${result3}=                 execute procedure       SelectAllCustomersWithTotalChildren    ${params}
    log                         ${result3}

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