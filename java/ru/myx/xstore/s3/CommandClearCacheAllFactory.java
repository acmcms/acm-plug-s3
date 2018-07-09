package ru.myx.xstore.s3;

import java.util.Collections;

import ru.myx.ae1.control.Control;
import ru.myx.ae1.control.MultivariantString;
import ru.myx.ae1.storage.PluginRegistry;
import ru.myx.ae1.storage.StorageImpl;
import ru.myx.ae3.act.Context;
import ru.myx.ae3.base.Base;
import ru.myx.ae3.base.BaseNativeObject;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.control.AbstractContainer;
import ru.myx.ae3.control.ControlContainer;
import ru.myx.ae3.control.command.ControlCommand;
import ru.myx.ae3.control.command.ControlCommandset;
import ru.myx.ae3.exec.Exec;
import ru.myx.ae3.produce.ObjectFactory;

/*
 * Created on 22.01.2005
 */
/** @author myx */
public final class CommandClearCacheAllFactory implements ObjectFactory<Object, ControlContainer<?>> {

	static final class CreateContainer extends AbstractContainer<CreateContainer> {

		private final StorageLevel3 parent;

		CreateContainer(final StorageLevel3 parent) {

			this.parent = parent;
		}

		@Override
		public Object getCommandResult(final ControlCommand<?> command, final BaseObject arguments) {

			{
				final BaseObject result = new BaseNativeObject()//
						.putAppend("storage", this.parent.getMnemonicName())//
				;
				this.parent.getServer().logQuickTaskUsage("XDS_COMMAND_STORAGE(3)_CC", result);
			}
			return this.parent.clearCacheAll();
		}

		@Override
		public ControlCommandset getCommands() {

			final String storageName = this.parent.getMnemonicName();
			return Control.createOptionsSingleton(
					Control.createCommand(
							"quick",
							MultivariantString.getString("<b>Clear </b>" + storageName + "<b> caches", Collections.singletonMap("ru", "<b>Очистить кэши: </b>" + storageName)))
							.setCommandPermission("publish").setCommandIcon("command-dispose").setAttribute(
									"description",
									MultivariantString
											.getString("Storage cache cleanup and type reload", Collections.singletonMap("ru", "Очистка кэшей хранилища и перезагрузка типов"))));
		}
	}

	private static final Class<?>[] TARGETS = {
			ControlContainer.class
	};

	private static final String[] VARIETY = {
			"XDS_COMMAND_STORAGE(3)_CC"
	};

	@Override
	public final boolean accepts(final String variant, final BaseObject attributes, final Class<?> source) {

		return true;
	}

	@Override
	public final ControlContainer<?> produce(final String variant, final BaseObject attributes, final Object source) {

		final String storageName = Base.getString(attributes, "storage", null);
		if (storageName == null) {
			return null;
		}
		final StorageImpl parent = PluginRegistry.getPlugin(Context.getServer(Exec.currentProcess()), storageName);
		if (parent == null || !(parent instanceof StorageLevel3)) {
			return null;
		}
		return new CreateContainer((StorageLevel3) parent);
	}

	@Override
	public final Class<?>[] sources() {

		return null;
	}

	@Override
	public final Class<?>[] targets() {

		return CommandClearCacheAllFactory.TARGETS;
	}

	@Override
	public final String[] variety() {

		return CommandClearCacheAllFactory.VARIETY;
	}
}
