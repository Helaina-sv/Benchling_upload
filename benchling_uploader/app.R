library(shiny)
library(httr)
library(jsonlite)
library(lubridate)
library(future)
library(promises)
library(doParallel)
library(dplyr)
library(dbplyr)
library(dotenv)
library(RPostgres)
library(DBI)
library(readxl)
library(shinyjs)

# Load environment variables
if (file.exists(".env")) {
  load_dot_env(".env")
} else {
  stop(".env file does not exist in the current working directory")
}

# Database connection details
db <- Sys.getenv("DB")
host_db <- Sys.getenv("HOST_DB")
db_port <- Sys.getenv("DB_PORT")
db_user <- Sys.getenv("DB_USER")
db_password <- Sys.getenv("DB_PASSWORD")
api_key <- "sk_xRhZUegRsZUH6mUPwcdnRqfxdQYRf"

benchcon <- dbConnect(RPostgres::Postgres(), dbname = db, host = host_db, port = db_port, user = db_user, password = db_password)

# Define the request function
get_response <- function(url, query_params, secret_key) {
  response <- GET(url,
                  query = query_params,
                  add_headers(.headers = c(
                    "accept" = "application/json"
                  )),
                  authenticate(secret_key, "")
  )
  return(response)
}

parse_response <- function(response) {
  parsed_content <- fromJSON(
    content(
      response,
      as = "text"
    )
  )
  return(parsed_content)
}

# Fetch schema details
url <- "https://helaina.benchling.com/api/v2/assay-result-schemas/assaysch_bSdwxZvH"
query_params <- list()
parsed_response <- parse_response(get_response(url, query_params, api_key))
schema_details <- parsed_response$fieldDefinitions

# Shiny UI
ui <- fluidPage(
  useShinyjs(),
  titlePanel("File Upload and API Integration"),
  sidebarLayout(
    sidebarPanel(
      fileInput("file", "Choose Excel File", accept = c(".xlsx")),
      actionButton("upload", "Upload and Process"),
      actionButton("check_columns", "Check Columns")
    ),
    mainPanel(
      tableOutput("contents"),
      verbatimTextOutput("response"),
      uiOutput("modal_ui")
    )
  )
)

# Shiny Server
server <- function(input, output, session) {
  observeEvent(input$file, {
    req(input$file)
    df <- read_excel(input$file$datapath)
    output$contents <- renderTable({
      df
    })
    
    observeEvent(input$check_columns, {
      user_columns <- colnames(df)
      expected_columns <- schema_details$displayName
      missing_columns <- setdiff(expected_columns, user_columns)
      column_types <- schema_details$type
      expected_types <- schema_details %>% select(displayName, type)
      
      if (length(missing_columns) == 0) {
        showModal(modalDialog(
          title = "Column Check",
          "All columns match the expected schema!",
          footer = modalButton("OK")
        ))
      } else {
        showModal(modalDialog(
          title = "Column Check",
          "The following columns do not match:",
          renderText({
            paste(missing_columns, collapse = ", ")
          }),
          renderText({
            paste("Expected data types for each column: ", 
                  paste(expected_types$displayName, ": ", expected_types$type, collapse = ", "), 
                  collapse = "\n")
          }),
          footer = modalButton("OK")
        ))
      }
    })
  })
  
  observeEvent(input$upload, {
    req(input$file)
    df <- read_excel(input$file$datapath)
    
    strain_query <- paste0("'", paste(df$Strain, collapse = "','"), "'")
    sql_query <- sprintf("SELECT id, \"file_registry_id$\" FROM strain WHERE \"file_registry_id$\" IN (%s)", strain_query)
    strain_key <- dbGetQuery(benchcon, sql_query)
    colnames(strain_key) <- c("strain", "Strain")
    
    run_query <- paste0("'", paste(df$`Run ID`, collapse = "','"), "'")
    sql_query_run <- sprintf("SELECT id, \"name$\" FROM usp_run_id$raw WHERE \"name$\" IN (%s)", run_query)
    run_id_key <- dbGetQuery(benchcon, sql_query_run)
    colnames(run_id_key) <- c("run_id", "Run ID")
    
    exp_query <- paste0("'", paste(df$`Experiment ID`, collapse = "','"), "'")
    sql_query_exp <- sprintf("SELECT id, \"name$\" FROM usp_experiment_id WHERE \"name$\" IN (%s)", exp_query)
    exp_id_key <- dbGetQuery(benchcon, sql_query_exp)
    colnames(exp_id_key) <- c("experiment_id", "Experiment ID")
    
    df <- df %>%
      left_join(strain_key, by = "Strain") %>%
      left_join(run_id_key, by = c("Run ID" = "Run ID")) %>%
      left_join(exp_id_key, by = c("Experiment ID" = "Experiment ID")) %>%
      select(-Strain, -`Run ID`, -`Experiment ID`)
    colnames(df) <- c("Value 1", "Value 2", "Strain", "Run ID", "Experiment ID")
    
    output$contents <- renderTable({
      df
    })
    
    projectID <- "src_TidCEmsN"
    schemaID <- "assaysch_bSdwxZvH"
    endpointURL <- "https://helaina.benchling.com/api/v2/assay-results:bulk-create"
    
    response <- get_response(url = "https://helaina.benchling.com/api/v2/entries",
                             query_params = list(
                               projectId = projectID,
                               pageSize = 50
                             ),
                             secret_key = api_key
    )
    
    available_entries <- parse_response(response)$entries[, c("id", "name", "apiURL")]
    
    response <- get_response(url = "https://helaina.benchling.com/api/v2/entries/etr_uGxP7kVB",
                             query_params = list(),
                             secret_key = api_key
    )
    
    parsedtableresponse <- parse_response(response)
    tableID <- parsedtableresponse$entry$days$notes[[1]]$apiId[parsedtableresponse$entry$days$notes[[1]]$type == "results_table"]
    
    create_payload <- function(df, schema_id, project_id, table_id) {
      assay_results <- lapply(1:nrow(df), function(i) {
        list(
          fields = list(
            "strain" = list(value = df$Strain[i]),
            "run_id" = list(value = df$`Run ID`[i]),
            "experiment_id" = list(value = df$`Experiment ID`[i]),
            "value_1" = list(value = (df$`Value 1`[i])),
            "value_2" = list(value = df$`Value 2`[i])
          ),
          schemaId = schema_id,
          projectId = project_id
        )
      })
      list(assayResults = assay_results, tableId = table_id)
    }
    
    payload <- create_payload(df, schemaID, projectID, tableID)
    json_payload <- toJSON(payload, auto_unbox = TRUE, pretty = TRUE)
    
    send_request <- function(endpoint_url, secret_key, json_payload) {
      response <- POST(
        url = endpoint_url,
        add_headers(.headers = c(
          "accept" = "application/json"
        )),
        authenticate(secret_key, ""),
        body = json_payload
      )
      response
    }
    
    response <- send_request(endpoint_url = endpointURL, secret_key = api_key, json_payload)
    
    output$response <- renderPrint({
      content(response, as = "text")
    })
  })
}

shinyApp(ui, server)
