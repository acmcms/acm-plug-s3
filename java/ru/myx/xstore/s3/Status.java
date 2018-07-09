/*
 * Created on 10.04.2006
 */
package ru.myx.xstore.s3;

import ru.myx.ae3.status.StatusInfo;
import ru.myx.ae3.status.StatusProvider;

final class Status implements StatusProvider {

	private final StorageLevel3 parent;

	Status(final StorageLevel3 parent) {

		this.parent = parent;
	}

	@Override
	public final StatusProvider[] childProviders() {

		return null;
	}

	@Override
	public final String statusDescription() {

		return "S3 storage (id=" + this.parent.getMnemonicName() + ")";
	}

	@Override
	public final void statusFill(final StatusInfo data) {

		this.parent.statusFill(data);
	}

	@Override
	public final String statusName() {

		return this.parent.getMnemonicName();
	}
}
