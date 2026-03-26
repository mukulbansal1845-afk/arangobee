package com.github.arangobee.changeset;

import com.arangodb.entity.BaseDocument;

import java.util.Date;

/**
 * Entry in the changes collection log
 * Type: entity class.
 *
 * @author lstolowski
 * @since 27/07/2014
 */
public class ChangeEntry {
    public static final String KEY_CHANGEID = "changeId";
    public static final String KEY_AUTHOR = "author";
    public static final String KEY_TIMESTAMP = "timestamp";
    public static final String KEY_CHANGELOGCLASS = "changeLogClass";
    public static final String KEY_CHANGESETMETHOD = "changeSetMethod";

    private final String changeId;
    private final String author;
    private final Date timestamp;
    private final String changeLogClass;
    private final String changeSetMethodName;

    public ChangeEntry(String changeId, String author, Date timestamp, String changeLogClass, String changeSetMethodName) {
        this.changeId = changeId;
        this.author = author;
        this.timestamp = new Date(timestamp.getTime());
        this.changeLogClass = changeLogClass;
        this.changeSetMethodName = changeSetMethodName;
    }

    public BaseDocument buildFullDBObject() {
        BaseDocument entry = new BaseDocument();

        entry.addAttribute(KEY_CHANGEID, this.changeId);
        entry.addAttribute(KEY_AUTHOR, this.author);
        entry.addAttribute(KEY_TIMESTAMP, this.timestamp);
        entry.addAttribute(KEY_CHANGELOGCLASS, this.changeLogClass);
        entry.addAttribute(KEY_CHANGESETMETHOD, this.changeSetMethodName);

        return entry;
    }

    @Override
    public String toString() {
        return "[ChangeSet: id=" + this.changeId + ", author=" + this.author + ", changeLogClass=" + this.changeLogClass + ", changeSetMethod="
            + this.changeSetMethodName + "]";
    }

    public String getChangeId() {
        return this.changeId;
    }

    public String getAuthor() {
        return this.author;
    }

    public Date getTimestamp() {
        return this.timestamp;
    }

    public String getChangeLogClass() {
        return this.changeLogClass;
    }

    public String getChangeSetMethodName() {
        return this.changeSetMethodName;
    }

}
