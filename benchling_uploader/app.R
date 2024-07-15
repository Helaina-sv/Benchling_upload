library(shiny)
library(httr)
library(jsonlite)
library(lubridate)
library(plyr)
library(dplyr)
library(dbplyr)
library(dotenv)
library(RPostgres)
library(DBI)
library(readxl)
library(shinyjs)
library(DT)

library(openxlsx)

create_dummy_data<- function(){
  all_schemas<- dbGetQuery(benchcon, "SELECT * FROM schema")
  all_entity<- dbGetQuery(benchcon, "SELECT * FROM entity")
  all_dropdown<- dbGetQuery(benchcon, "SELECT * FROM dropdown_option$raw")
  all_dropdown_menu<- dbGetQuery(benchcon, "SELECT * FROM dropdown")
  all_users<- dbGetQuery(benchcon, "SELECT * FROM user$raw")
  all_entries<- dbGetQuery(benchcon, "SELECT * FROM entry")
  all_tables<- dbGetQuery(benchcon, "SELECT * FROM information_schema.tables WHERE table_type = 'BASE TABLE'")
  schema_details<- fetch_schema_details("assaysch_k6vDZiRO")
  df<- read_excel("~/Desktop/HTS USP TEST.xlsx")
  df<- remove_empty_columns(df, schema_details)
  
  box<- dbGetQuery(benchcon, "SELECT * FROM well_qpix_re_array_source$raw")
  container<- dbGetQuery(benchcon, "SELECT * FROM container$raw")
  
  
  hts <- dbGetQuery(benchcon, "
  SELECT * 
  FROM hts_usp_sampling_id_results$raw
  WHERE cell_source_unique_id_inventory IS NOT NULL
  ORDER BY \"modified_at$\" DESC
  LIMIT 500
")
  
  
}

load_dot_env(".env")


# Database connection details
db <- Sys.getenv("DB")
host_db <- Sys.getenv("HOST_DB")
db_port <- Sys.getenv("DB_PORT")
db_user <- Sys.getenv("DB_USER")
db_password <- Sys.getenv("DB_PASSWORD")
#api_key<- Sys.getenv("BENCHLINGAPIKEY")
api_key <- "sk_xRhZUegRsZUH6mUPwcdnRqfxdQYRf"

benchcon <- dbConnect(RPostgres::Postgres(), dbname = db, host = host_db, port = db_port, user = db_user, password = db_password)


all_projects<- dbGetQuery(benchcon, "SELECT id,name FROM project$raw") 
colnames(all_projects)<- c("source_id", "project_name")
all_projects<- all_projects %>%
  filter(project_name %in% c("Early-Stage R&D Team",
                             "Late Stage R&D",
                             # "Nutritional Biology and Safety",
                             "Bioinformatics",
                             "Analytical Chemistry",
                             "Protein Characterization from External Partners"))

get_entries <- function(projectid) {
  query <- paste0("SELECT id, name FROM entry$raw WHERE source_id LIKE '", projectid, "'")
  all_entries <- dbGetQuery(benchcon, query)
  
  return(all_entries)
}


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

# Function to fetch schema details based on the schema ID
fetch_schema_details <- function(schema_id) {
  url <- paste0("https://helaina.benchling.com/api/v2/assay-result-schemas/", schema_id)
  query_params <- list()
  response <- get_response(url, query_params, api_key)
  parsed_response <- parse_response(response)
  schema_details <- parsed_response$fieldDefinitions
  return(schema_details)
}
# if table is > 500 rows
split_into_chunks <- function(df, chunk_size) {
  split(df, ceiling(seq_along(1:nrow(df)) / chunk_size))
}
#function to check missing values
check_missing_values <- function(df) {
  columns_with_missing <- list()
  
  for (coln in colnames(df)) {
    if (any(is.na(df[[coln]]))) {
      columns_with_missing[[length(columns_with_missing) + 1]] <- coln
    }
  }
  
  if (length(columns_with_missing) > 0) {
    return(data.frame(Column = unlist(columns_with_missing), stringsAsFactors = FALSE))
  } else {
    return(NULL)
  }
}


get_table_id<- function(entry_id, schema_id){
  url<- paste0("https://helaina.benchling.com/api/v2/entries/",entry_id)
  response <- get_response(url = url,
                           query_params = list(),
                           secret_key = api_key
  )
  
  parsedtableresponse<- parse_response(response)
  notes_df <- parsedtableresponse$entry$days$notes[[1]]
  
  # Filter and select the required columns
  result_df <- notes_df %>%
    filter(type == "results_table") %>%
    select(assayResultSchemaId, apiId)%>%
    filter(assayResultSchemaId %in% schema_id)%>%
    select(apiId)
  
  as.character(result_df$apiId)
  
}
get_schema_id_from_entry<- function(entry_id){
  url<- paste0("https://helaina.benchling.com/api/v2/entries/",entry_id)
  response <- get_response(url = url,
                           query_params = list(),
                           secret_key = api_key
  )
  parsedtableresponse<- parse_response(response)
  notes_df <- parsedtableresponse$entry$days$notes[[1]]
  
  # Filter and select the required columns
  
  result_df <- notes_df %>%
    filter(type == "results_table") 
  if(nrow(result_df) == 0){
    return(NULL)
  }else{
    result_df<- result_df %>%
      select(assayResultSchemaId, apiId)
    
    as.character(result_df$assayResultSchemaId)
  }
  
  
}
create_json_payload <- function(df, schema_id, project_id, table_id, schema_details) {
  
  schema_details <- schema_details %>%
    filter(displayName %in% colnames(df))
  
  # Extract the display names, internal names, and types from the schema_details dataframe
  display_names <- schema_details$displayName
  internal_names <- schema_details$name
  types <- schema_details$type
  
  # Identify columns that should be treated as text
  text_columns <- internal_names[types == "text"]
  
  # Function to create a single record payload
  create_record_payload <- function(row, display_names, internal_names, text_columns) {
    fields <- setNames(lapply(seq_along(display_names), function(i) {
      value <- row[[display_names[i]]]
      if (!is.na(value)) {
        # Convert to character if the field is in the text_columns
        if (internal_names[i] %in% text_columns) {
          value <- as.character(value)
        }
        return(list(value = value))
      } else {
        return(NULL)
      }
    }), internal_names)
    
    # Remove NULL entries (where value was NA)
    fields <- fields[!sapply(fields, is.null)]
    
    list(fields = fields, schemaId = schema_id, projectId = project_id)
  }
  
  # Create the list of assay results
  assay_results <- lapply(1:nrow(df), function(i) {
    create_record_payload(df[i, ], display_names, internal_names, text_columns)
  })
  
  # Ensure the payload has the correct structure
  payload <- list(assayResults = assay_results, tableId = table_id)
  
  # Convert to JSON and ensure proper formatting
  json_payload <- toJSON(payload, auto_unbox = TRUE, pretty = TRUE)
  return(json_payload)
}
remove_empty_columns<- function(df, schema_details){
  required<- schema_details %>% 
    filter(isRequired == FALSE)%>%
    filter(displayName %in% colnames(df))
  
  for (name in required$displayName ){
    if(all(is.na(df[[name]]))){
      df<- df %>%
        select(- all_of(name))
    }
  }
  return(df)
}

create_dropdown_index<- function(df, schema_details){
  dropdown_links <- schema_details %>%
    filter(displayName %in% colnames(df))%>%
    filter(type %in% c("dropdown")) %>%
    select(name, displayName, type)
  query <- paste0("'", paste(dropdown_links$displayName, collapse = "','"), "'")
  sql_query <- paste("SELECT id FROM dropdown WHERE name IN (", query, ")")
  dropdown_id <- dbGetQuery(benchcon, sql_query)
  
  query <- paste0("'", paste(dropdown_id, collapse = "','"), "'")
  sql_query <- paste("SELECT id, name FROM dropdown_option$raw WHERE dropdown_id IN (", query, ")")
  dropdown_id <- dbGetQuery(benchcon, sql_query)
  
  return(dropdown_id)
}
# Function to check custom entity links
check_custom_entity_links <- function(df, benchcon, schema_details) {
  entity_links <- schema_details %>%
    filter(displayName %in% colnames(df)) %>%
    filter(type %in% c("custom_entity_link", "entity_link", "dropdown", "storage_link")) %>%
    select(name, displayName, type)
  
  entity_links_displayname <- as.vector(entity_links$displayName)
  
  entity_data <- df %>%
    select(all_of(entity_links_displayname))
  
  entity_data <- sapply(entity_data, function(x) unique(x[!is.na(x)]))
  
  entity_match <- function(vec) {
    if (length(vec) == 0) {
      return(character(0))
    }
    query <- paste0("'", paste(vec, collapse = "','"), "'")
    
    sql_query <- sprintf("SELECT \"file_registry_id$\" FROM strain WHERE \"file_registry_id$\" IN (%s)", query)
    check1 <- dbGetQuery(benchcon, sql_query)$`file_registry_id$`
    
    sql_query <- sprintf("SELECT name FROM entity WHERE name IN (%s)", query)
    check2 <- dbGetQuery(benchcon, sql_query)$name
    
    check3 <- create_dropdown_index(df, schema_details)$name
    
    sql_query <- sprintf("SELECT barcode FROM container$raw WHERE name IN (%s)", query)
    check4 <- dbGetQuery(benchcon, sql_query)$barcode
    
    setdiff(vec, c(check1, check2, check3, check4))
  }
  
  unmatched <- lapply(entity_data, entity_match)
  
  return(unmatched)
}

# Function to convert custom entity links to internal IDs
convert_custom_entity_links <- function(df, schema_details) {
  
  
  # Get the entity links details from the schema and filter for existing columns
  entity_links <- schema_details %>%
    filter(displayName %in% colnames(df)) %>%
    filter(type %in% c("custom_entity_link", "entity_link", "dropdown","storage_link")) %>%
    select(name, displayName)
  
  entity_links_displayname <- entity_links$displayName
  
  # Collect unique non-NA values from the entity link columns
  entity_data <- df %>%
    select(all_of(entity_links_displayname)) %>%
    unique() %>%
    unlist() %>%
    .[!is.na(.)]
  
  if (length(entity_data) > 0) {
    query <- paste0("'", paste(entity_data, collapse = "','"), "'")
    
    # Query to get matching entities from the database
    sql_query1 <- sprintf("SELECT id, \"file_registry_id$\" AS name FROM strain WHERE \"file_registry_id$\" IN (%s)", query)
    check1 <- dbGetQuery(benchcon, sql_query1)
    
    sql_query2 <- sprintf("SELECT id, name FROM entity WHERE name IN (%s)", query)
    check2 <- dbGetQuery(benchcon, sql_query2)
    
    
    check3 <- create_dropdown_index(df, schema_details)
    
    sql_query <- sprintf("SELECT id, barcode AS name FROM container$raw WHERE barcode IN (%s)", query)
    check4 <- dbGetQuery(benchcon, sql_query)
    
    index <- rbind(check1, check2, check3, check4)
    
    # Replace entity link values in the dataframe with their corresponding ids
    for (display_name in entity_links_displayname) {
      df[[display_name]] <- sapply(df[[display_name]], function(x) {
        if (is.na(x)) {
          return(NA)
        }
        match_value <- index$id[match(x, index$name)]
        if (is.na(match_value)) 
          return(NA) 
        else 
          return(match_value)
      })
    }
  }
  
  # The dataframe is already filtered, no need to intersect with schema details again
  return(df)
}



create_df_for_payload <- function(df, benchcon, schema_details) {
  unmatched <- check_custom_entity_links(df, benchcon, schema_details)
  df_empty_columns_removed<- remove_empty_columns(df, schema_details)
  df_filtered <- remove_unmatched_rows(df_empty_columns_removed, unmatched)
  df_filtered_converted <- convert_custom_entity_links(df_filtered, benchcon, schema_details)
  return(df_filtered_converted)
}

# Function to remove unmatched rows
remove_unmatched_rows <- function(df, unmatched) {
  for (column_name in names(unmatched)) {
    unmatched_values <- unmatched[[column_name]]
    if (length(unmatched_values) > 0) {
      df <- df[!(df[[column_name]] %in% unmatched_values), ]
    }
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
  response <- GET(url = paste0(url, "/", taskid), 
                  add_headers(.headers = c("accept" = "application/json")), 
                  authenticate(api_key, ""))
  response_data <- content(response, "parsed")
  
  if (!is.null(response_data$message)) {
    error_messages <- if (!is.null(response_data$error)) {
      # Extract error messages and format them into a dataframe
      do.call(rbind, lapply(response_data$error, function(e) {
        data.frame(Index = e$index, Message = e$message, stringsAsFactors = FALSE)
      }))
    } else {
      data.frame(Index = NA, Message = "No additional error details available.", stringsAsFactors = FALSE)
    }
    status_df <- data.frame(Status = response_data$status, Message = response_data$message, stringsAsFactors = FALSE)
    result <- list(status_df = status_df, error_details = error_messages)
  } else {
    result <- list(status_df = data.frame(Status = response_data$status, Message = "", stringsAsFactors = FALSE), error_details = NULL)
  }
  
  return(result)
}

##Logic to get project ID and notebook entries and then table IDs from it


# Shiny UI
ui <- fluidPage(
  theme = shinythemes::shinytheme("cerulean"),
  useShinyjs(),
  titlePanel("Benchling Uploader"),
  sidebarLayout(
    sidebarPanel(
      selectInput("project", "Select a Project:", 
                  choices = setNames(all_projects$source_id, all_projects$project_name),
                  selected = NULL),
      actionButton("goproject","Show notebook entries"),
      uiOutput("entry_ui"),
      fluidRow(
        column(
          width = 5,
          uiOutput("goentry"),
        ),
        column(
          width = 2,
          uiOutput("notebook_link"),
          tags$script(
            'Shiny.addCustomMessageHandler("jsCode", function(message) {
      eval(message.code);
    });'
          )
        ),
      ),
      uiOutput("schema_ui"),
      uiOutput("download_template"),
      
      fluidRow(
        column(
          width = 12,  # Adjust as needed
          uiOutput("file_uploader")
        )
        
      ),
      fluidRow(
        column(
          width = 8,
          uiOutput("tab_selector")
        ),
        column(
          width = 4,  # Adjust as needed
          div(style = "margin-top: 25px;", uiOutput("run_qc"))  # Adjust margin as needed
        )
      ),
      fluidRow(
        column(12, 
               div(style = "text-align: center;", 
                   actionButton("upload", "Upload to Benchling", class = "btn btn-primary", style = "display: none;")
               )
        )
      )
      ,
      
      
      uiOutput("task_ui")
    ),
    mainPanel(
      wellPanel(
        h3("Quality check", style = "margin-top: 0;"),
        
        
        DTOutput("status_table")
      ),
      wellPanel(
        tabsetPanel(
          tabPanel("Data Uploaded", DTOutput("sheet_contents")),
          tabPanel("Data Passed QC", DTOutput("qc_contents"))
        )
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
    output$run_qc <- renderUI({
      actionButton("qc", "Run QC")
    })
    
  })
  observeEvent(input$project, {
    # Reset UI elements when a new project is selected
    updateSelectInput(session, "entry", selected = NULL)
    updateSelectInput(session, "schema", selected = NULL)
    output$entry_ui <- renderUI(NULL)
    output$notebook_link <- renderUI(NULL)
    output$goentry <- renderUI(NULL)
    output$schema_ui <- renderUI(NULL)
    output$file_uploader <- renderUI(NULL)
    output$run_qc <- renderUI(NULL)
    shinyjs::hide("upload")
    
    # Clear the data table
    output$status_table <- DT::renderDataTable(NULL)
    output$sheet_contents <- DT::renderDataTable(NULL)
    output$qc_contents <- DT::renderDataTable(NULL)
    
    # Reset QC panel
    output$response <- renderText("")
    output$task_status <- renderText("")
  })
  observeEvent(input$entry, {
    # Reset UI elements when a new project is selected
    output$notebook_link <- renderUI(NULL)
    output$file_uploader <- renderUI(NULL)
    output$run_qc <- renderUI(NULL)
    output$schema_ui <- renderUI(NULL)
    # output$download_button<- renderUI(NULL)
    #output$download_template <- renderUI(NULL)
    
    # Hide the upload button
    shinyjs::hide("upload")
    
    # Clear the data table
    output$status_table <- DT::renderDataTable(NULL)
    output$sheet_contents <- DT::renderDataTable(NULL)
    output$qc_contents <- DT::renderDataTable(NULL)
    
    # Reset QC panel
    output$response <- renderText("")
    output$task_status <- renderText("")
  })
  observeEvent(input$schema, {
    # Reset UI elements when a new project is selected
    output$run_qc <- renderUI(NULL)
    
    shinyjs::hide("upload")
    
    # Clear the data table
    output$status_table <- DT::renderDataTable(NULL)
    output$sheet_contents <- DT::renderDataTable(NULL)
    output$qc_contents <- DT::renderDataTable(NULL)
    
    # Reset QC panel
    output$response <- renderText("")
    output$task_status <- renderText("")
  })
  observeEvent(input$goproject, {
    query <- paste0("SELECT id, name FROM entry$raw WHERE source_id LIKE '", input$project, "'")
    all_entries <- dbGetQuery(benchcon, query)
    
    output$entry_ui <- renderUI({
      selectizeInput(
        "entry",
        "Select Notebook Entry:",
        choices = setNames(all_entries$id, all_entries$name),
        selected = NA,
        options = list(placeholder = 'Type to search...', create = FALSE)
      )
    })
  })
  observeEvent(input$entry, {
    output$goentry <- renderUI({
      actionButton("showschemas", "Show available schemas")
    })
  })
  
  observeEvent(input$schema,{
    req(input$schema)
    output$file_uploader<- renderUI({
      fileInput("file", "Upload file", accept = c(".xlsx"))
    })
  })
  
  observeEvent(input$file, {
    req(input$file)
    
    # Get sheet names from the uploaded file
    sheets <- excel_sheets(input$file$datapath)
    
    # Update the UI for tab selection
    output$tab_selector <- renderUI({
      selectInput("selected_tab", "Choose Tab:", choices = sheets)
    })
  })
  observeEvent(input$schema, {
    output$download_template <- renderUI({
      if (!is.null(input$schema)) {
        downloadButton("download_schema_template", "Download Schema Template")
      } else {
        NULL  # Return NULL if no schema is selected
      }
    })
  })
  
  schema_details <- reactive({
    req(input$schema)  
    selected_schema_id <- input$schema
    fetch_schema_details(selected_schema_id)
  })
  
  
  output$download_schema_template <- downloadHandler(
    filename = function() {
      "schema_template.xlsx"
    },
    content = function(file) {
      details <- schema_details()  # Call the reactive expression
      
      # Debug statement
      cat("Preparing schema template for download\n")
      print(details)
      
      # Create a new workbook
      wb <- createWorkbook()
      addWorksheet(wb, "Sheet1")
      
      # Write the display names as column headers
      writeData(wb, sheet = "Sheet1", x = as.data.frame(t(details$displayName)), colNames = FALSE, rowNames = FALSE)
      
      # Write the types as the first row of data
      writeData(wb, sheet = "Sheet1", x = as.data.frame(t(details$type)), startRow = 2, colNames = FALSE, rowNames = FALSE)
      
      # Save the workbook to the specified file
      saveWorkbook(wb, file, overwrite = TRUE)
      
      # Debug statement
      cat("Schema template downloaded successfully\n")
    }
  )
  
  
  observeEvent(input$showschemas, {
    req(input$entry)
    schemas <- get_schema_id_from_entry(input$entry)
    if(!is.null(schemas)){
      query <- paste0("'", paste(schemas, collapse = "','"), "'")
      
      # Construct the SQL query using the IN clause
      sql_query <- paste0("SELECT id, name FROM schema WHERE id IN (", query, ")")
      
      # Execute the SQL query
      all_schema <- dbGetQuery(benchcon, sql_query)
      output$notebook_select <- renderUI({
        selectInput("notebook", "Select Notebook entry", choices = setNames(available_entries$id, available_entries$name), selected = NULL)
      })
      
      output$schema_ui <- renderUI({
        selectInput("schema", 
                    "Following schemas were found in the selected notebook entry. Please make a selection:", 
                    choices = setNames(all_schema$id, all_schema$name), 
                    selected = character(0))
      })
    }
  })
  
  data <- reactive({
    req(input$file, input$selected_tab)
    read_excel(input$file$datapath, sheet = input$selected_tab)
  })
  
  
  ##QC function
  observeEvent(input$qc, {
    req(input$file)
    df <- data()
    df_uploaded <- df
    output$sheet_contents <- renderDT({
      datatable(df_uploaded, options = list(scrollX = TRUE))
    })
    
    required_columns <- schema_details() %>%
      filter(isRequired == TRUE) %>%
      select(displayName)
    required_columns <- as.character(required_columns$displayName)
    
    if (!all(required_columns %in% colnames(df))) {
      showModal(modalDialog(
        title = "",
        paste("Data cannot be uploaded because required columns are missing. Please ensure that following columns are present:", 
              paste(required_columns, collapse = ", ")),
        easyClose = TRUE,
        footer = tagList(
          modalButton("Ok")
        )
      ))
    }
    
    # Automatically check columns when file is uploaded
    user_columns <- colnames(df)
    expected_columns <- schema_details()$displayName
    missing_columns <- setdiff(expected_columns, user_columns)
    matching_columns <- intersect(expected_columns, user_columns)
    
    df <- df %>%
      select(all_of(matching_columns))
    
    if (length(user_columns) != ncol(df)) {
      showModal(modalDialog(
        title = "Columns Mismatch",
        paste("There are columns in your uploaded file that are either missing or do not match the expected schema structure. Unmatched columns have been dropped. While you can still upload the columns that match, please ensure that it is the intended and correct file. Following columns were expected but not found in your uploaded file: ", 
              paste(missing_columns, collapse = ", ")),
        easyClose = TRUE,
        footer = tagList(
          modalButton("Acknowledge and Proceed")
        )
      ))
    }
    
    status_data <- data.frame(
      Step = character(),
      Status = character(),
      Details = character(),
      stringsAsFactors = FALSE
    )
    
    if (length(missing_columns) == 0) {
      status_data <- rbind(status_data, data.frame(Step = "Column names check", Status = "Passed", Details = "All column names match the expected schema."))
    } else {
      missing_columns_text <- paste(missing_columns, collapse = ", ")
      status_data <- rbind(status_data, data.frame(Step = "Column names check", Status = "Failed, but matching columns can be uploaded", Details = paste("Missing columns:", missing_columns_text)))
      shinyjs::show("upload")
    }
    
    if (all(required_columns %in% colnames(df))) {
      status_data <- rbind(status_data, data.frame(Step = "Required Columns check", Status = "Passed", Details = "All required columns are present."))
    } else {
      status_data <- rbind(status_data, data.frame(Step = "Required Columns check", Status = "Failed. Data cannot be uploaded", Details = paste("Required columns are:", required_columns)))
      shinyjs::hide("upload")
    }
    
    # Check for missing values
    missing_values_check <- check_missing_values(df)
    
    if (is.null(missing_values_check)) {
      status_data <- rbind(status_data, data.frame(Step = "Missing values check", Status = "Passed", Details = "No missing values found in the dataset."))
    } else {
      missing_values_details <- paste(missing_values_check$Column, collapse = ", ")
      status_data <- rbind(status_data, data.frame(Step = "Missing values check", Status = "Failed, but present values can be uploaded", Details = paste("Columns with missing values:", missing_values_details)))
    }
    
    if (length(matching_columns) == 0) {
      showModal(modalDialog(
        title = "Schema Mismatch: Nothing to be uploaded!",
        paste("None of the columns match the expected schema structure. The expected columns in the selected schema are:", 
              paste(expected_columns, collapse = ", ")),
        easyClose = TRUE,
        footer = modalButton("Acknowledge and reupload")
      ))
      req(FALSE)
    } else {
      df <- remove_empty_columns(df, schema_details())
      custom_entity_check <- check_custom_entity_links(df, benchcon, schema_details())
      
      if (all(sapply(custom_entity_check, length) == 0)) {
        status_data <- rbind(status_data, data.frame(Step = "Custom entity check", Status = "Passed", Details = "All custom entity links matched successfully."))
        shinyjs::show("upload")
        df_filtered<- df
      } else {
        unmatched <- custom_entity_check
        df_filtered <- remove_unmatched_rows(df, unmatched)
        
        # Create a detailed message for the unmatched entities
        unmatched_details <- lapply(names(unmatched), function(column_name) {
          unmatched_values <- unmatched[[column_name]]
          if (length(unmatched_values) > 0) {
            paste0(column_name, ": ", paste(unmatched_values, collapse = ", "))
          } else {
            NULL
          }
        })
        unmatched_details <- unmatched_details[!sapply(unmatched_details, is.null)]
        unmatched_message <- paste(unmatched_details, collapse = "; ")
        
        showModal(modalDialog(
          title = "Unmatched Custom Entity Links",
          paste("Unmatched custom entity links were found. Rows containing those links have been removed. Please review the data that passed QC before proceeding or fix the links and upload a new file. Unmatched entities: ", unmatched_message),
          footer = modalButton("OK")
        ))
        
        status_data <- rbind(status_data, data.frame(Step = "Custom entity check", Status = "Failed, rows containing unmatched entities were removed", 
                                                     Details = paste("Unmatched custom entity links have been removed. Please review the filtered data and either reupload an updated file or proceed with the table. Unmatched entities:", unmatched_message)
        )
        )
      }
      
      shinyjs::show("upload")
      
      output$qc_contents <- renderDT({
        datatable(df_filtered, options = list(scrollX = TRUE))
      })
      
      status_data <- rbind(status_data, data.frame(Step = "QC Summary", Status = "", Details = paste("Rows uploaded = ", 
                                                                                                     nrow(df_uploaded), 
                                                                                                     ". Columns uploaded = ",
                                                                                                     ncol(df_uploaded),
                                                                                                     ". Rows passed QC = ", nrow(df_filtered),
                                                                                                     ". Columns passed QC = ", ncol(df_filtered),
                                                                                                     ". Columns removed = ", paste(setdiff(colnames(df_uploaded), colnames(df_filtered)), collapse = ", ")
      )
      )
      )
    }
    
    output$status_table <- renderDT({
      datatable(status_data, options = list(dom = 't', pageLength = nrow(status_data)))
    })
    
    response <- get_response(url = "https://helaina.benchling.com/api/v2/entries", query_params = list(projectId = input$project, pageSize = 50), secret_key = api_key)
    available_entries <<- parse_response(response)$entries[, c("id", "name")]
    
    
  })
  
  
  
  
  
  
  selected_entry_url <- reactive({
    req(input$entry)
    entry_id <- input$entry
    dbGetQuery(benchcon, sprintf("SELECT url FROM entry WHERE id LIKE '%s'", entry_id))
  })
  
  
  observe({
    url <- selected_entry_url()
    if (!is.null(url) && nrow(url) > 0) {
      output$notebook_link <- renderUI({
        actionButton("go_button", "Go to entry")
      })
    } else {
      output$notebook_link <- renderUI({
        "No notebook selected"
      })
    }
  })
  observeEvent(input$go_button, {
    url <- selected_entry_url()
    if (!is.null(url) && nrow(url) > 0) {
      js_code <- sprintf("window.open('%s', '_blank')", url$url[1])
      session$sendCustomMessage(type = 'jsCode', list(code = js_code))
    }
  })
  
  task_ids <- reactiveVal(list())
  task_statuses <- reactiveValues()
  
  
  observeEvent(input$upload, {
    req(input$file)
    req(input$entry)
    req(input$project)
    req(input$schema)
    
    df <- data()
    cat("Upload button clicked.\n")
    
    schemaID <- input$schema
    projectID <- input$project
    entryID <- input$entry
    tableID <- get_table_id(entryID, schemaID)
    
    required_columns<- schema_details()%>%
      filter(isRequired == TRUE)%>%
      select(displayName)
    required_columns<- as.character(required_columns$displaName)
    
    # Automatically check columns when file is uploaded
    user_columns <- colnames(df)
    expected_columns <- schema_details()$displayName
    missing_columns <- setdiff(expected_columns, user_columns)
    matching_columns <- intersect(expected_columns, user_columns)
    
    df<- df %>%
      select(all_of(matching_columns))
    
    endpointURL <- "https://helaina.benchling.com/api/v2/assay-results:bulk-create"
    
    df<- remove_empty_columns(df, schema_details())
    unmatched <- check_custom_entity_links(df =  df,benchcon = benchcon,schema_details =  schema_details())
    
    df_filtered <- remove_unmatched_rows(df = df, unmatched)
    
    df_filtered_converted <- convert_custom_entity_links(df_filtered, schema_details())
    df_filtered_converted <- as.data.frame(df_filtered_converted)
    # Split the dataframe into chunks of 499 rows
    chunks <- split_into_chunks(df_filtered_converted, 500)
    if (length(chunks) > length(tableID)) {
      showModal(modalDialog(
        title = "Insufficient Tables",
        paste("The data you are uploading has",nrow(df_filtered_converted ),"rows and will not fit in the available tables \n",
              "Please use the link to the notebook entry and insert", length(chunks) - length(tableID), 
              "more result schema(s).")
      ))
    } else {
      showModal(modalDialog(
        title = "Uploading",
        paste("The data you are uploading has",nrow(df_filtered_converted ),"rows",
              "and it will fit in",length(chunks), "result schema(s). Please use check task id button to check the status of upload")
      ))
      task_ids_list <- list()
      for (i in seq_along(chunks)) {
        json_payload <- create_json_payload(chunks[[i]], 
                                            schema_id = schemaID, 
                                            project_id = projectID,
                                            table_id = tableID[i], 
                                            schema_details = schema_details())
        
        
        
        response <- send_request(endpoint_url = endpointURL, secret_key = api_key, json_payload)
        
        task_id <- parse_response(response)$taskId
        task_ids_list[[i]] <- task_id
        
        # Initialize task status
        task_statuses[[task_id]] <- "Pending"
        
        # Update the reactive value
        task_ids(task_ids_list)
      }
    }
  })
  
  output$task_ui <- renderUI({
    tasks <- task_ids()
    if (length(tasks) == 0) {
      return(NULL)
    }
    
    do.call(tagList, lapply(seq_along(tasks), function(i) {
      wellPanel(
        textInput(paste0("task_id_", i), paste("Task ID", i), value = tasks[[i]]),
        actionButton(paste0("check_task_", i), "Check Task Status"),
        DTOutput(paste0("status_", tasks[[i]])),
        DTOutput(paste0("errors_", tasks[[i]]))
      )
    }))
  })
  
  
  observe({
    tasks <- task_ids()
    for (i in seq_along(tasks)) {
      local({
        task_id <- tasks[[i]]
        observeEvent(input[[paste0("check_task_", i)]], {
          status_result <- get_task_status(task_id, api_key)
          task_statuses[[paste0("status_", task_id)]] <- status_result$status_df
          task_statuses[[paste0("errors_", task_id)]] <- status_result$error_details
          
          output[[paste0("status_", task_id)]] <- renderDT({
            datatable(task_statuses[[paste0("status_", task_id)]], options = list(dom = 't', paging = FALSE))
          })
          
          output[[paste0("errors_", task_id)]] <- renderDT({
            datatable(task_statuses[[paste0("errors_", task_id)]], options = list(dom = 't', paging = FALSE))
          })
        })
      })
    }
  })
  
  
  
}

shinyApp(ui, server)