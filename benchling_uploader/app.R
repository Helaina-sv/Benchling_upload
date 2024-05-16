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
library(DT)

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

# Function to match column names and types
match_column_names <- function(df) {
  expected_names <- schema_details %>%
    filter(!type == "custom_entity_link") %>%
    select(displayName, type)
  
  actualnames <- data.frame(
    displayName = colnames(df),
    actual_type = sapply(df, class),
    row.names = NULL
  )
  
  actualnames <- actualnames %>%
    filter(displayName %in% expected_names$displayName)
  
  actualnames$actual_type <- gsub("character", "text", actualnames$actual_type)
  
  df_to_return <- plyr::join(expected_names, actualnames)
  colnames(df_to_return) <- c("Column name", "Expected data type", "Uploaded data type")
  
  mismatched <- df_to_return %>%
    filter(`Expected data type` != `Uploaded data type`)
  
  if (nrow(mismatched) == 0) {
    return(TRUE)
  } else {
    return(mismatched)
  }
}

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




# Function to check custom entity links
check_custom_entity_links <- function(df, benchcon) {
  # Initialize result list
  unmatched <- list()
  
  # Strain check
  strain_query <- paste0("'", paste(df$Strain, collapse = "','"), "'")
  sql_query <- sprintf("SELECT \"file_registry_id$\" FROM strain WHERE \"file_registry_id$\" IN (%s)", strain_query)
  strain_key <- dbGetQuery(benchcon, sql_query)$`file_registry_id$`
  unmatched$strain <- setdiff(df$Strain, strain_key)
  
  # Run ID check
  run_query <- paste0("'", paste(df$`Run ID`, collapse = "','"), "'")
  sql_query_run <- sprintf("SELECT \"name$\" FROM usp_run_id$raw WHERE \"name$\" IN (%s)", run_query)
  run_id_key <- dbGetQuery(benchcon, sql_query_run)$`name$`
  unmatched$run_id <- setdiff(df$`Run ID`, run_id_key)
  
  # Experiment ID check
  exp_query <- paste0("'", paste(df$`Experiment ID`, collapse = "','"), "'")
  sql_query_exp <- sprintf("SELECT \"name$\" FROM usp_experiment_id WHERE \"name$\" IN (%s)", exp_query)
  exp_id_key <- dbGetQuery(benchcon, sql_query_exp)$`name$`
  unmatched$exp_id <- setdiff(df$`Experiment ID`, exp_id_key)
  
  return(unmatched)
}

# Function to convert custom entity links to internal IDs
convert_custom_entity_links <- function(df) {
  # Strain conversion
  strain_query <- paste0("'", paste(df$Strain, collapse = "','"), "'")
  sql_query <- sprintf("SELECT id, \"file_registry_id$\" FROM strain WHERE \"file_registry_id$\" IN (%s)", strain_query)
  strain_key <- dbGetQuery(benchcon, sql_query)
  colnames(strain_key) <- c("strain", "Strain")
  
  # Run ID conversion
  run_query <- paste0("'", paste(df$`Run ID`, collapse = "','"), "'")
  sql_query_run <- sprintf("SELECT id, \"name$\" FROM usp_run_id$raw WHERE \"name$\" IN (%s)", run_query)
  run_id_key <- dbGetQuery(benchcon, sql_query_run)
  colnames(run_id_key) <- c("run_id", "Run ID")
  
  # Experiment ID conversion
  exp_query <- paste0("'", paste(df$`Experiment ID`, collapse = "','"), "'")
  sql_query_exp <- sprintf("SELECT id, \"name$\" FROM usp_experiment_id WHERE \"name$\" IN (%s)", exp_query)
  exp_id_key <- dbGetQuery(benchcon, sql_query_exp)
  colnames(exp_id_key) <- c("experiment_id", "Experiment ID")
  
  # Merge with the original dataframe
  df <- df %>%
    left_join(strain_key, by = "Strain") %>%
    left_join(run_id_key, by = c("Run ID" = "Run ID")) %>%
    left_join(exp_id_key, by = c("Experiment ID" = "Experiment ID")) %>%
    select(-Strain, -`Run ID`, -`Experiment ID`)
  
  # Rename columns to match expected schema
  colnames(df) <- c("Value 1", "Value 2", "Strain", "Run ID", "Experiment ID")
  
  return(df)
}
create_df_for_payload<- function(df){
  unmatched <- check_custom_entity_links(df, benchcon)
  df_filtered<- remove_unmatched_rows(df, unmatched)
  df_filtered_converted<- convert_custom_entity_links(df_filtered)
  return(df_filtered_converted)
}



# Function to remove unmatched rows
remove_unmatched_rows <- function(df, unmatched) {
  if (length(unmatched$strain) > 0) {
    df <- df[!(df$Strain %in% unmatched$strain), ]
  }
  if (length(unmatched$run_id) > 0) {
    df <- df[!(df$`Run ID` %in% unmatched$run_id), ]
  }
  if (length(unmatched$exp_id) > 0) {
    df <- df[!(df$`Experiment ID` %in% unmatched$exp_id), ]
  }
  return(df)
}

# Function to send request
send_request <- function(endpoint_url, secret_key, json_payload) {
  response <- POST(
    url = endpoint_url,
    add_headers(.headers = c(
      "accept" = "application/json",
      "Content-Type" = "application/json"
    )),
    authenticate(secret_key, ""),
    body = json_payload,
    encode = "json"
  )
  response
}

# Function to get task status
get_task_status <- function(taskid, api_key) {
  url <- "https://helaina.benchling.com/api/v2/tasks"
  response <- get_response(url = paste0(url, "/", taskid),
                           query_params = list(),
                           secret_key = api_key)
  result <- parse_response(response)$status
  return(result)
}

# Shiny UI
ui <- fluidPage(
  useShinyjs(),
  titlePanel("File Upload and API Integration"),
  sidebarLayout(
    sidebarPanel(
      fileInput("file", "Choose Excel File", accept = c(".xlsx")),
      actionButton("upload", "Upload and Process", style = "display:none;"),
      uiOutput("notebook_select"),
      uiOutput("notebook_link"),
      uiOutput("task_ui")
    ),
    mainPanel(
      wellPanel(
        DTOutput("status_table")
      ),
      wellPanel(
        DTOutput("contents")
      ),
      verbatimTextOutput("response"),
      textOutput("task_status")
    )
  )
)

# Shiny Server
server <- function(input, output, session) {
  available_entries <- NULL
  task_id <- NULL
  
  observeEvent(input$file, {
    req(input$file)
    df <- read_excel(input$file$datapath)
    
    # Automatically check columns when file is uploaded
    user_columns <- colnames(df)
    expected_columns <- schema_details$displayName
    missing_columns <- setdiff(expected_columns, user_columns)
    
    status_data <- data.frame(
      Step = character(),
      Status = character(),
      Details = character(),
      stringsAsFactors = FALSE
    )
    
    if (length(missing_columns) == 0) {
      status_data <- rbind(status_data, data.frame(Step = "Column names check", Status = "Passed", Details = "All column names match the expected schema."))
      type_check <- match_column_names(df)
      
      if (isTRUE(type_check)) {
        status_data <- rbind(status_data, data.frame(Step = "Data types check", Status = "Passed", Details = "All data types match the expected schema."))
        custom_entity_check <- check_custom_entity_links(df, benchcon)
        
        if (all(sapply(custom_entity_check, length) == 0)) {
          status_data <- rbind(status_data, data.frame(Step = "Custom entity links check", Status = "Passed", Details = "All custom entity links matched successfully."))
          shinyjs::show("upload")
          output$contents <- renderDT({
            datatable(df)
          })
        } else {
          if (length(custom_entity_check$strain) > 0) {
            status_data <- rbind(status_data, data.frame(Step = "Custom entity links check", Status = "Failed", Details = paste("Unmatched Strains:", paste(custom_entity_check$strain, collapse = ", "))))
          }
          if (length(custom_entity_check$run_id) > 0) {
            status_data <- rbind(status_data, data.frame(Step = "Custom entity links check", Status = "Failed", Details = paste("Unmatched Run IDs:", paste(custom_entity_check$run_id, collapse = ", "))))
          }
          if (length(custom_entity_check$exp_id) > 0) {
            status_data <- rbind(status_data, data.frame(Step = "Custom entity links check", Status = "Failed", Details = paste("Unmatched Experiment IDs:", paste(custom_entity_check$exp_id, collapse = ", "))))
          }
          
          df_filtered <<- remove_unmatched_rows(df, custom_entity_check)
          
          showModal(modalDialog(
            title = "Unmatched Custom Entity Links",
            "Unmatched custom entity links were found. Rows containing those links have been removed. Please review the data that passed QC before proceeding or fix the links and upload a new file.",
            footer = modalButton("OK")
          ))
          
          output$contents <- renderDT({
            datatable(df_filtered)
          })
          
          status_data <- rbind(status_data, data.frame(Step = "Warning", Status = "Rows Removed", Details = "Unmatched custom entity links have been removed. Please review the filtered data and either reupload an updated file or proceed with the table."))
          
          shinyjs::show("upload")
        }
      } else {
        type_check_df <- as.data.frame(type_check)
        type_check_details <- paste(
          apply(type_check_df, 1, function(row) {
            paste("Column name:", row["Column name"], "- Expected:", row["Expected data type"], "- Uploaded:", row["Uploaded data type"])
          }),
          collapse = "; "
        )
        status_data <- rbind(status_data, data.frame(Step = "Data types check", Status = "Failed", Details = type_check_details))
        shinyjs::hide("upload")
        output$contents <- renderDT({
          NULL
        })
      }
    } else {
      missing_columns_text <- paste(missing_columns, collapse = ", ")
      expected_columns_text <- paste(expected_columns, collapse = ", ")
      status_data <- rbind(status_data, data.frame(Step = "Column names check", Status = "Failed", Details = paste("Missing columns:", missing_columns_text, ". Expected columns:", expected_columns_text)))
      shinyjs::hide("upload")
      output$contents <- renderDT({
        NULL
      })
    }
    
    output$status_table <- renderDT({
      datatable(status_data, options = list(dom = 't', pageLength = nrow(status_data)))
    })
    
    # Fetch available notebook entries
    response <- get_response(url = "https://helaina.benchling.com/api/v2/entries",
                             query_params = list(
                               projectId = "src_TidCEmsN",  # Adjust with the correct project ID
                               pageSize = 50
                             ),
                             secret_key = api_key
    )
    
    available_entries <<- parse_response(response)$entries[, c("id", "name")]
    
    output$notebook_select <- renderUI({
      selectInput("notebook", "Select Notebook entry", choices = setNames(available_entries$id, available_entries$name), selected = NULL)
    })
  })
  
  observeEvent(input$notebook, {
    req(input$notebook)
    entry_id <- input$notebook
    selected_entry_url <- dbGetQuery(benchcon, sprintf("SELECT url FROM entry WHERE id LIKE '%s'", entry_id))
    
    output$notebook_link <- renderUI({
      if (nrow(selected_entry_url) > 0) {
        HTML(paste(
          "Notebook URL: <a href='", selected_entry_url$url, "' target='_blank'>", selected_entry_url$url, "</a>"
        ))
      } else {
        "No notebook selected"
      }
    })
  })
  
  # Reactive to get the project ID based on the selected notebook name
  project_id <- reactive({
    req(input$notebook)
    id <- available_entries %>%
      filter(id == input$notebook) %>%
      select(id)
    as.character(id$id)
  })
  
  observe({
    print(project_id())
  })
  observeEvent(input$upload, {
    req(input$file)
    req(input$notebook)
    df <- read_excel(input$file$datapath)
    
    # Convert to character to ensure user-uploaded values are shown
    df$Strain <- as.character(df$Strain)
    df$`Run ID` <- as.character(df$`Run ID`)
    df$`Experiment ID` <- as.character(df$`Experiment ID`)
    
    schemaID <- "assaysch_bSdwxZvH"
    endpointURL <- "https://helaina.benchling.com/api/v2/assay-results:bulk-create"
    
    # Create the JSON payload
    df_filtered_converted<- create_df_for_payload(df)
    payload <- create_payload(df_filtered_converted, schemaID, projectID, tableID)
    
    json_payload <- toJSON(payload, auto_unbox = TRUE, pretty = TRUE)
    print(json_payload)
    response <- send_request(endpoint_url = endpointURL, secret_key = api_key, json_payload)
    
    task_id <<- parse_response(response)$taskId
    
    output$response <- renderPrint({
      content(response, as = "text")
    })
    
    output$task_ui <- renderUI({
      wellPanel(
        textInput("task_id", "Task ID", value = task_id),
        actionButton("check_task", "Check Task Status")
      )
    })
  })
  
  observeEvent(input$check_task, {
    req(input$task_id)
    task_status <- get_task_status(input$task_id, api_key)
    
    output$task_status <- renderText({
      paste("Task Status:", task_status)
    })
  })
}

shinyApp(ui, server)
