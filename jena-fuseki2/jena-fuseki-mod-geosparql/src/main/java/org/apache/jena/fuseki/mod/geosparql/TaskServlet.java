package org.apache.jena.fuseki.mod.geosparql;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@WebServlet(urlPatterns = {"/status", "/events", "/start", "/cancel"}, asyncSupported = true)
public class TaskServlet extends HttpServlet {

    private static final Object lock = new Object();
    private volatile boolean running = false;
    private String message = "";
    private Set<AsyncContext> listeners = ConcurrentHashMap.newKeySet();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {

        String path = req.getServletPath();

        if ("/status".equals(path)) {
            handleStatus(req, resp);
        } else if ("/events".equals(path)) {
            handleEvents(req, resp);
        } else {
            resp.sendError(HttpServletResponse.SC_NOT_FOUND);
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {
        String path = req.getServletPath();

        if ("/start".equals(path)) {
            handleStart(resp);
        } else if ("/cancel".equals(path)) {
            handleCancel(resp);
        } else {
            resp.sendError(HttpServletResponse.SC_NOT_FOUND);
        }
    }

    private void handleStatus(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {
        resp.setContentType("application/json");
        resp.setCharacterEncoding("UTF-8");

        String json = String.format(
            "{\"running\": %b, \"message\": \"%s\"}", running, message
        );
        resp.getWriter().write(json);
    }

    private void handleEvents(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        resp.setContentType("text/event-stream");
        resp.setCharacterEncoding("UTF-8");
        resp.setHeader("Cache-Control", "no-cache");

        final AsyncContext asyncContext = req.startAsync();
        asyncContext.setTimeout(0);
        listeners.add(asyncContext);
    }

    private void handleStart(HttpServletResponse resp) throws IOException {
        synchronized (lock) {
            if (running) {
                resp.sendError(HttpServletResponse.SC_CONFLICT, "Task already running.");
                return;
            }
            running = true;
            message = "Task started";
        }

        broadcast("taskStarted", "{\"progress\": 0}");

        // simulate task execution
        new Thread(() -> {
            try {
                Thread.sleep(3000); // simulate work
                synchronized (lock) {
                    running = false;
                    message = "Task completed successfully";
                }
                broadcast("taskDone", "{\"success\": true, \"message\": \"All done!\"}");
            } catch (InterruptedException e) {
                synchronized (lock) {
                    running = false;
                    message = "Task was cancelled";
                }
                broadcast("taskFailed", "{\"success\": false, \"message\": \"Cancelled\"}");
            }
        }).start();

        resp.setStatus(HttpServletResponse.SC_ACCEPTED);
    }

    private void handleCancel(HttpServletResponse resp) throws IOException {
        // Not implemented: you’d need a way to interrupt the task thread
        // or set a flag to stop gracefully
        resp.sendError(HttpServletResponse.SC_NOT_IMPLEMENTED, "Cancel not implemented");
    }

    private void broadcast(String event, String jsonData) {
        for (AsyncContext context : listeners) {
            try {
                PrintWriter writer = context.getResponse().getWriter();
                writer.write("event: " + event + "\n");
                writer.write("data: " + jsonData + "\n\n");
                writer.flush();
            } catch (IOException e) {
                listeners.remove(context);
                context.complete();
            }
        }
    }
}
