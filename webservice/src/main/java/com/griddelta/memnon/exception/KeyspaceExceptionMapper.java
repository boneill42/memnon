//
// Copyright (c) 2012 Health Market Science, Inc.
//
package com.griddelta.memnon.exception;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;

@Provider
public class KeyspaceExceptionMapper implements ExceptionMapper<InvalidConfigurationInQueryException> {
    public Response toResponse(InvalidConfigurationInQueryException exception) {
        return Response.status(Status.NOT_FOUND).entity(exception.getCause()).build();
    }
}
