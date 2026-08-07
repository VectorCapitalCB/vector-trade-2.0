package cl.vc.service.admin.servlet;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/** Lightweight endpoint used by the embedded admin UI to validate its Bearer token. */
public class AdminAuthServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        res.getWriter().write("{\"authenticated\":true}");
    }
}
