package com.example.SnowpipeRest.rest;

import com.example.SnowpipeRest.utils.EnqueueResponse;
import com.example.SnowpipeRest.utils.InvalidPayloadResponse;
import com.example.SnowpipeRest.utils.TableNotFoundResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/snowpipe")
public class Resource {

  static final IngestEngine ingestEngine = new IngestEngine();

  static {
    ingestEngine.init();
  }

  @PutMapping("/insert/{database}/{schema}/{table}")
  @ResponseBody
  public EnqueueResponse insert(
      @PathVariable String database,
      @PathVariable String schema,
      @PathVariable String table,
      @RequestBody String body) {
    return ingestEngine.enqueueData(database, schema, table, body);
  }

  @ExceptionHandler(TableNotFoundResponse.class)
  @ResponseStatus(HttpStatus.NOT_FOUND)
  public ResponseEntity<String> handleTableNotFound(TableNotFoundResponse e) {
    return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
  }

  @ExceptionHandler(InvalidPayloadResponse.class)
  @ResponseStatus(HttpStatus.NOT_FOUND)
  public ResponseEntity<String> handleBadJson(InvalidPayloadResponse e) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
  }
}
