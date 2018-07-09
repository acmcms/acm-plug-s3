/*
 * Created on 10.05.2006
 */
package ru.myx.xstore.s3.concept;

import java.util.Set;

import ru.myx.ae3.base.BaseObject;

/**
 * @author myx
 * 
 */
public interface Transaction {
	/**
	 * @param guid
	 * @param aliasAdd
	 * @param aliasRemove
	 * @throws Throwable
	 */
	void aliases(final String guid, final Set<String> aliasAdd, final Set<String> aliasRemove) throws Throwable;
	
	/**
     * 
     */
	void commit();
	
	/**
	 * @param local
	 * @param ctnLnkId
	 * @param lnkId
	 * @param objId
	 * @param name
	 * @param folder
	 * @param created
	 * @param owner
	 * @param state
	 * @param title
	 * @param typeName
	 * @param data
	 * @param versionId
	 * @param versionComment
	 * @param versionData
	 * @throws Throwable
	 */
	void create(
			final boolean local,
			final String ctnLnkId,
			final String lnkId,
			final String objId,
			final String name,
			final boolean folder,
			final long created,
			final String owner,
			final int state,
			final String title,
			final String typeName,
			final BaseObject data,
			final String versionId,
			final String versionComment,
			final BaseObject versionData) throws Throwable;
	
	/**
	 * @param link
	 * @param soft
	 * @throws Throwable
	 */
	void delete(final LinkData link, final boolean soft) throws Throwable;
	
	/**
	 * @param local
	 * @param ctnLnkId
	 * @param lnkId
	 * @param name
	 * @param folder
	 * @param linkedIdentity
	 * @throws Throwable
	 */
	void link(
			final boolean local,
			final String ctnLnkId,
			final String lnkId,
			final String name,
			final boolean folder,
			final String linkedIdentity) throws Throwable;
	
	/**
	 * @param link
	 * @param ctnLnkId
	 * @param key
	 * @throws Throwable
	 */
	void move(final LinkData link, final String ctnLnkId, final String key) throws Throwable;
	
	/**
	 * @param linkedIdentity
	 * @throws Throwable
	 */
	void record(final String linkedIdentity) throws Throwable;
	
	/**
	 * @param link
	 * @param key
	 * @throws Throwable
	 */
	void rename(final LinkData link, final String key) throws Throwable;
	
	/**
	 * @param lnkId
	 * @throws Throwable
	 */
	void resync(final String lnkId) throws Throwable;
	
	/**
	 * @param link
	 * @param historyId
	 * @param folder
	 * @param created
	 * @param state
	 * @param title
	 * @param typeName
	 * @throws Throwable
	 */
	void revert(
			final LinkData link,
			final String historyId,
			final boolean folder,
			final long created,
			final int state,
			final String title,
			final String typeName) throws Throwable;
	
	/**
	 * @param link
	 * @param historyId
	 * @param folder
	 * @param created
	 * @param state
	 * @param title
	 * @param typeName
	 * @param removed
	 * @param added
	 * @throws Throwable
	 */
	void revert(
			final LinkData link,
			final String historyId,
			final boolean folder,
			final long created,
			final int state,
			final String title,
			final String typeName,
			final BaseObject removed,
			final BaseObject added) throws Throwable;
	
	/**
     * 
     */
	void rollback();
	
	/**
	 * @param guid
	 * @param linkedIdentityOld
	 * @param linkedIdentityNew
	 * @throws Throwable
	 */
	void segregate(final String guid, final String linkedIdentityOld, final String linkedIdentityNew) throws Throwable;
	
	/**
	 * @param link
	 * @param soft
	 * @throws Throwable
	 */
	void unlink(final LinkData link, final boolean soft) throws Throwable;
	
	/**
	 * @param link
	 * @param linkedIdentity
	 * @throws Throwable
	 */
	void update(final LinkData link, final String linkedIdentity) throws Throwable;
	
	/**
	 * @param link
	 * @param linkedIdentity
	 * @param versionId
	 * @param folder
	 * @param created
	 * @param state
	 * @param title
	 * @param typeName
	 * @param ownership
	 * @throws Throwable
	 */
	void update(
			final LinkData link,
			final String linkedIdentity,
			final String versionId,
			final boolean folder,
			final long created,
			final int state,
			final String title,
			final String typeName,
			boolean ownership) throws Throwable;
	
	/**
	 * @param link
	 * @param linkedIdentity
	 * @param versionId
	 * @param folder
	 * @param created
	 * @param state
	 * @param title
	 * @param typeName
	 * @param ownership
	 * @param removed
	 * @param added
	 * @throws Throwable
	 */
	void update(
			final LinkData link,
			final String linkedIdentity,
			final String versionId,
			final boolean folder,
			final long created,
			final int state,
			final String title,
			final String typeName,
			boolean ownership,
			final BaseObject removed,
			final BaseObject added) throws Throwable;
	
	/**
	 * @param objId
	 * @throws Throwable
	 */
	void versionClearAll(final String objId) throws Throwable;
	
	/**
	 * @param versionId
	 * @param versionParentId
	 * @param versionComment
	 * @param objectId
	 * @param title
	 * @param typeName
	 * @param owner
	 * @param versionData
	 * @throws Throwable
	 */
	void versionCreate(
			final String versionId,
			final String versionParentId,
			final String versionComment,
			final String objectId,
			final String title,
			final String typeName,
			final String owner,
			final BaseObject versionData) throws Throwable;
	
	/**
	 * @param versionId
	 * @param versionComment
	 * @param objectId
	 * @param title
	 * @param typeName
	 * @param owner
	 * @param versionData
	 * @throws Throwable
	 */
	void versionStart(
			final String versionId,
			final String versionComment,
			final String objectId,
			final String title,
			final String typeName,
			final String owner,
			final BaseObject versionData) throws Throwable;
}
