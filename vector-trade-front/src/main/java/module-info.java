module front {

    requires javafx.controls;
    requires javafx.fxml;
    requires com.gluonhq.attach.util;
    requires com.gluonhq.attach.display;
    requires com.gluonhq.attach.storage;
    requires javafx.graphics;
    requires akka.actor;
    requires javafx.base;
    requires protobuf.java.util;
    requires protobuf.java;
    requires resteasy.client;
    requires keycloak.admin.client;
    requires keycloak.core;
    requires principal.module;
    requires org.controlsfx.controls;
    requires org.apache.httpcomponents.httpclient;
    requires java.desktop;

    requires java.base;
    requires java.se;
    requires org.slf4j;
    requires com.fasterxml.jackson.core;
    requires typesafe.config;
    requires static lombok;
    requires Enzo;
    requires javafx.media;
    requires org.json;
    requires org.update4j;
    requires javafx.web;
    requires jlayer;
    requires com.fasterxml.jackson.databind;
    requires org.apache.httpcomponents.httpcore;
    requires org.eclipse.jetty.websocket.api;
    requires org.eclipse.jetty.websocket.client;
    requires org.java_websocket;
    requires org.eclipse.jetty.websocket.common;
    requires dev.mccue.guava.collect;
    requires com.google.gson;
    requires org.jfree.jfreechart;
    requires org.jfree.chart.fx;
    requires ta4j.core;


    opens view to java.base;
    opens libs.win to java.base;
    opens blotter.img to java.base;
    opens cl.vc.blotter.adaptor to akka.actor;
    opens cl.vc.blotter to javafx.graphics;
    opens cl.vc.blotter.controller to javafx.fxml;
    opens sounds to java.base;
    opens cl.vc.blotter.model to javafx.base;


    exports cl.vc.blotter.controller;
    exports cl.vc.blotter.utils;
    opens cl.vc.blotter.utils to javafx.base, javafx.fxml, javafx.graphics;
}

