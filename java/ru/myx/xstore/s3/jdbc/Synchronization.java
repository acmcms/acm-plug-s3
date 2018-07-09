/**
 *
 */
package ru.myx.xstore.s3.jdbc;

import java.util.Set;

import ru.myx.ae1.storage.BaseSync;
import ru.myx.ae3.help.Create;

final class Synchronization implements BaseSync {

	private final SynchronizerJdbc jdbc;

	private final String guid;

	private final Set<String> exportListOriginal;

	private final Set<String> importListOriginal;

	private final Set<String> exportList;

	private final Set<String> importList;

	Synchronization(final SynchronizerJdbc jdbc, final String guid, final Set<String> exportList, final Set<String> importList) {

		this.jdbc = jdbc;
		this.guid = guid;
		this.exportListOriginal = exportList;
		this.importListOriginal = importList;
		this.exportList = Create.tempSet(exportList);
		this.importList = Create.tempSet(importList);
	}

	@Override
	public void clear() {

		this.exportList.clear();
		this.importList.clear();
	}

	@Override
	public void commit() {

		this.jdbc.commitChange(this.guid, this.exportListOriginal, this.importListOriginal, this.exportList, this.importList);
	}

	@Override
	public String[] getExportSynchronizations() {

		return this.exportList.toArray(new String[this.exportList.size()]);
	}

	@Override
	public String[] getImportSynchronizations() {

		return this.importList.toArray(new String[this.importList.size()]);
	}

	@Override
	public boolean isEmpty() {

		return this.exportList.isEmpty() && this.importList.isEmpty();
	}

	@Override
	public void synchronizeExport(final String guid) {

		this.exportList.add(guid);
	}

	@Override
	public void synchronizeExportCancel(final String guid) {

		this.exportList.remove(guid);
	}

	@Override
	public void synchronizeFill(final BaseSync synchronization) {

		for (final String guid : this.exportList) {
			synchronization.synchronizeExport(guid);
		}
		for (final String guid : this.importList) {
			synchronization.synchronizeImport(guid);
		}
	}

	@Override
	public void synchronizeImport(final String guid) {

		this.importList.add(guid);
	}

	@Override
	public void synchronizeImportCancel(final String guid) {

		this.importList.remove(guid);
	}
}
