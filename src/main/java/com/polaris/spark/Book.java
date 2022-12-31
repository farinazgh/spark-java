package com.polaris.spark;

import java.util.Date;

public class Book {
    int id;
    int authorId;
    String title;
    Date releaseDate;
    String link;


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }


    public int getAuthorId() {
        return authorId;
    }

    public void setAuthorId(int authorId) {
        this.authorId = authorId;
    }


    public void setAuthorId(Integer authorId) {
        if (authorId == null) {
            this.authorId = 0;
        } else {
            this.authorId = authorId;
        }
    }


    public String getTitle() {
        return title;
    }


    public void setTitle(String title) {
        this.title = title;
    }


    public Date getReleaseDate() {
        return releaseDate;
    }


    public void setReleaseDate(Date releaseDate) {
        this.releaseDate = releaseDate;
    }


    public String getLink() {
        return link;
    }


    public void setLink(String link) {
        this.link = link;
    }
}
