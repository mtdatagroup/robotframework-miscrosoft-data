*** Settings ***

Resource            resources/common.robot
Library             OperatingSystem

Test Setup          Connect to Database     AdventureWorks      ${trusted_connection_string}
Test Teardown       Disconnect from Databases
Default Tags        Database        SQL Server

*** Test Cases ***
Interact with Connection
    ${connection_name}=         current connection name
    log                         ${connection_name}
    ${connections}=             list connections
    log                         ${connections}
    Connect to Database         Bob         ${trusted_connection_string}
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

Logon to SQL Server with username and password
    [Tags]                      Username and Password Authentication
    Connect to Database         trusted_conn            ${user_and_pass_connection_string}
    ${dict}=                    read query              SELECT TOP 10 * FROM dbo.DimCustomer
    Log                         ${dict}

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

