package com.amazonaws.samples;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.annotation.WebServlet;

import net.snowflake.client.jdbc.internal.javax.servlet.ServletException;
import net.snowflake.client.jdbc.internal.javax.servlet.http.HttpServlet;
import net.snowflake.client.jdbc.internal.javax.servlet.http.HttpServletRequest;
import net.snowflake.client.jdbc.internal.javax.servlet.http.HttpServletResponse;

@WebServlet("/BeginUpload")
public class BeginUpload extends HttpServlet{

	/**
	 * 
	 */
	private static final long serialVersionUID = -3602648104238701650L;

	public BeginUpload() {
        super();
    }
	
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException { 
        response.setContentType("text/html"); 
        PrintWriter out = response.getWriter(); 
        out.println("<html><body>"); 
        out.println("<h1>Hello Readers</h1>"); 
        out.println("</body></html>"); 
        response.setStatus(404);

    }
	
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException{
		
	}
}
