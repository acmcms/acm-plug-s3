import ru.myx.ae3.produce.Produce;
import ru.myx.xstore.s3.CommandClearCacheAllFactory;
import ru.myx.xstore.s3.CommandClearCacheTypesFactory;
import ru.myx.xstore.s3.PluginLevel3Factory;
import ru.myx.xstore.s3.local.CleanerTask;

/*
 * Created on 07.10.2003
 * 
 * To change the template for this generated file go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
/**
 * @author myx
 * 
 *         To change the template for this generated type comment go to
 *         Window>Preferences>Java>Code Generation>Code and Comments
 */
public final class Main {
	/**
	 * @param args
	 */
	public static final void main(final String[] args) {
		System.out.println( "RU.MYX.AE1PLUG.STORAGE(3): plugin: ACM [StorageL3] is being initialized..." );
		System.out.println( "RU.MYX.AE1PLUG.STORAGE(3): cleaner: " + CleanerTask.INSTANCE );
		Produce.registerFactory( new CommandClearCacheAllFactory() );
		Produce.registerFactory( new CommandClearCacheTypesFactory() );
		Produce.registerFactory( new PluginLevel3Factory() );
	}
}
