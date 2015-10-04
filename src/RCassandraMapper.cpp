#include <R.h>
#include <Rinternals.h>
#include <Rdefines.h>
#include <cassandra.h>
#include <stdio.h>
#include <vector>

CassCluster* cluster;
CassSession* session;
CassFuture* connect_future;

void processPage(const CassResult* result, std::vector<std::vector<const char*> > &results) {
  // Get result count
  size_t nbResults = cass_result_row_count(result);
  size_t nbColumns = cass_result_column_count(result);
  
  //PROTECT(results = allocVector(VECSXP, nbResults + 2));
  
  // Get column name
  //SET_VECTOR_ELT(results, 0, allocVector(STRSXP, nbColumns));
  if (results.size() == 0) {
    std::vector<const char*> columns;
    for (int i = 0; i < nbColumns; i++) {
      const char* string_value;
      size_t string_value_length;
      cass_result_column_name(result, i, &string_value, &string_value_length);
      //SET_STRING_ELT(VECTOR_ELT(results, 0), i, mkChar(string_value));
      columns.push_back(string_value);
    }
    results.push_back(columns);
    
    // Get column type
    std::vector<const char*> types;
    //SET_VECTOR_ELT(results, 1, allocVector(STRSXP, nbColumns));
    for (int i = 0; i < nbColumns; i++) {
      CassValueType columnType = cass_result_column_type(result, i);
      switch (columnType) {
      case CASS_VALUE_TYPE_BOOLEAN :
        types.push_back("logical");
        //SET_STRING_ELT(VECTOR_ELT(results, 1), i, mkChar("logical"));
        break;
      case CASS_VALUE_TYPE_DECIMAL :
        types.push_back("numeric");
        //SET_STRING_ELT(VECTOR_ELT(results, 1), i, mkChar("numeric"));
        break;
      case CASS_VALUE_TYPE_DOUBLE :
        types.push_back("numeric");
        //SET_STRING_ELT(VECTOR_ELT(results, 1), i, mkChar("numeric"));
        break;
      case CASS_VALUE_TYPE_FLOAT :
        types.push_back("numeric");
        //SET_STRING_ELT(VECTOR_ELT(results, 1), i, mkChar("numeric"));
        break;
      case CASS_VALUE_TYPE_INT :
        types.push_back("integer");
        //SET_STRING_ELT(VECTOR_ELT(results, 1), i, mkChar("integer"));
        break;
      case CASS_VALUE_TYPE_TIMESTAMP :
        types.push_back("integer");
        //SET_STRING_ELT(VECTOR_ELT(results, 1), i, mkChar("integer"));
        break;
      case CASS_VALUE_TYPE_VARCHAR:
      case CASS_VALUE_TYPE_TEXT :
        types.push_back("character");
        //SET_STRING_ELT(VECTOR_ELT(results, 1), i, mkChar("character"));
        break;
      default:
        types.push_back("UNKNOWN");
        //SET_STRING_ELT(VECTOR_ELT(results, 1), i, mkChar("UNKNOWN"));
      }
    }
    results.push_back(types);
  }
  
  // Start at 2 beacause there's column name and column index stored
  int count = 2;
  
  CassIterator* iterator = cass_iterator_from_result(result);
  
  while (cass_iterator_next(iterator)) {
    //SET_VECTOR_ELT(results, count, allocVector(STRSXP, nbColumns));
    std::vector<const char*> values;
    
    const CassRow* row = cass_iterator_get_row(iterator);
    CassIterator * rowIterator = cass_iterator_from_row(row);
    
    int columnCount = 0;
    while (cass_iterator_next(rowIterator)) {
      const CassValue * columnValue = cass_iterator_get_column(rowIterator);
      CassValueType columnType = cass_value_type(columnValue);
      //SET_STRING_ELT(VECTOR_ELT(results, count), columnCount, mkChar(""));
      if (cass_value_is_null(columnValue)) {
        values.push_back("");
      } else {
        char buffer[20];
        
        switch (columnType) {
        case CASS_VALUE_TYPE_BOOLEAN :
          break;
        case CASS_VALUE_TYPE_DECIMAL :
          break;
        case CASS_VALUE_TYPE_DOUBLE :
          cass_double_t double_value;
          cass_value_get_double (columnValue, &double_value);
          sprintf(buffer, "%lf", double_value);
          //SET_STRING_ELT(VECTOR_ELT(results, count), columnCount, mkChar(buffer));
          values.push_back(buffer);
          break;
        case CASS_VALUE_TYPE_FLOAT :
          cass_float_t float_value;
          cass_value_get_float (columnValue, &float_value);
          sprintf(buffer, "%f", float_value);
          //SET_STRING_ELT(VECTOR_ELT(results, count), columnCount, mkChar(buffer));
          values.push_back(buffer);
          break;
        case CASS_VALUE_TYPE_INT :
          cass_int32_t int_value;
          cass_value_get_int32(columnValue, &int_value);
          sprintf(buffer, "%d", int_value);
          //SET_STRING_ELT(VECTOR_ELT(results, count), columnCount, mkChar(buffer));
          values.push_back(buffer);
          break;
        case CASS_VALUE_TYPE_TIMESTAMP :
          break;
        case CASS_VALUE_TYPE_VARCHAR:
        case CASS_VALUE_TYPE_TEXT :
          const char* string_value;
          size_t string_value_length;
          cass_value_get_string(columnValue, &string_value, &string_value_length);
          //SET_STRING_ELT(VECTOR_ELT(results, count), columnCount, mkChar(string_value));
          values.push_back(string_value);
          break;
        default:
          values.push_back("");
        }
      }
      
      columnCount++;
    }
    cass_iterator_free(rowIterator);
    
    // Update counter
    results.push_back(values);
    count++;
  }
  
  cass_iterator_free(iterator);
  //UNPROTECT(1);
}

extern "C"{   
  SEXP connect(SEXP address) {
    SEXP result_code;
    
    cluster = cass_cluster_new();
	  session = cass_session_new();

	  cass_cluster_set_contact_points(cluster, CHAR(STRING_ELT(address,0)));

	  connect_future = cass_session_connect_keyspace(session, cluster, "icypricescrawler");

	  CassError rc = cass_future_error_code(connect_future);

	  printf("Connect result: %s\n", cass_error_desc(rc));
    
    PROTECT(result_code = NEW_INTEGER(1));
    INTEGER(result_code)[0] = 0;
    UNPROTECT(1);
    
    return result_code;
  }
  
  SEXP connectWithCredentials(SEXP address, SEXP username, SEXP password) {  
    SEXP result_code;
    
	  cluster = cass_cluster_new();
	  session = cass_session_new();

	  cass_cluster_set_credentials(cluster, CHAR(STRING_ELT(username,0)), CHAR(STRING_ELT(password,0)));
	  cass_cluster_set_contact_points(cluster, CHAR(STRING_ELT(address,0)));

	  connect_future = cass_session_connect(session, cluster);

	  CassError rc = cass_future_error_code(connect_future);

	  printf("Connect result: %s\n", cass_error_desc(rc));
    
    PROTECT(result_code = NEW_INTEGER(1));
    INTEGER(result_code)[0] = 0;
    UNPROTECT(1);
    
    return result_code;
  }
  
  SEXP executeQuery(SEXP query) {
    SEXP results;
    std::vector<std::vector<const char*> > tmpResults;
    const CassResult* result = NULL;
    CassFuture* future = NULL;
    
    CassStatement* statement = cass_statement_new(CHAR(STRING_ELT(query,0)), 0);
    
    /* To enable paging set the page size */
    cass_statement_set_paging_size(statement, 100);
    future = cass_session_execute(session, statement);
    
    /* Check future for error */
    
    result = cass_future_get_result(future);
    processPage(result, tmpResults);
    
    
    /* Check to see if there are any more pages left */
    while (cass_result_has_more_pages(result)) {
      cass_statement_set_paging_state(statement, result);
      
      cass_result_free(result);
      
      future = cass_session_execute(session, statement);
      
      result = cass_future_get_result(future);
      processPage(result, tmpResults);
    }
    cass_result_free(result);
    
    PROTECT(results = allocVector(VECSXP, tmpResults.size()));
    
    int rowCount = 0;
    for (std::vector<std::vector<const char*> >::iterator rows = tmpResults.begin(); rows != tmpResults.end(); ++rows) {
      std::vector<const char*> row = (*rows);
      SET_VECTOR_ELT(results, rowCount, allocVector(STRSXP, row.size()));
      int valueCount = 0;
      for (std::vector<const char*>::iterator values = row.begin(); values != row.end(); ++values) {
        SET_STRING_ELT(VECTOR_ELT(results, rowCount), valueCount, mkChar((*values)));
        valueCount++;
      }
      rowCount++;
    }
    
    UNPROTECT(1);
    return results;
  }
  
  SEXP disconnect() {
    SEXP result_code;
    
    cass_future_free(connect_future);
    cass_session_free(session);
    cass_cluster_free(cluster);
    
    PROTECT(result_code = NEW_INTEGER(1));
    INTEGER(result_code)[0] = 0;
    UNPROTECT(1);
    
    return result_code;
  }
}

/*CassCluster* cluster;
CassSession* session;
CassFuture* connect_future;*/
  
  /*SEXP connectWithCredentials(SEXP address, SEXP username, SEXP password) {
  char* raddress;
  raddress = SEXPtoString(address);
  printf("Connect result: %s\n", raddress);
	
	  cluster = cass_cluster_new();
	  session = cass_session_new();

	  cass_cluster_set_credentials(cluster, username, password);
	  cass_cluster_set_contact_points(cluster, address);

	  cass_session_connect(session, cluster);

	  CassError rc = cass_future_error_code(connect_future);

	  printf("Connect result: %s\n", cass_error_desc(rc));
    return R_NilValue;
  }*/
  
  /*SEXP connect() {
	  cluster = cass_cluster_new();
	  session = cass_session_new();

	  cass_cluster_set_contact_points(cluster, "127.0.0.1");

	  cass_session_connect(session, cluster);

	  CassError rc = cass_future_error_code(connect_future);

	  printf("Connect result: %s\n", cass_error_desc(rc));
    
    return R_NilValue;
  }
  
  SEXP executeQuery(const char* query) {
	  return R_NilValue;
  }
  
  SEXP executeParameterizedQuery(const char* query, char** parameters) {
	  return R_NilValue;
  }*/