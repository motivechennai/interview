import java.util.Map;

/**
 * 
 * @author interview2
 * 
 * 
 */
public interface ContactService {

	/**
	 * Adds a contact.
	 */
	public void addContact(Map<String, ContactVO> obj, ContactVO contactVO);

	/**
	 * Edit/Modify any existing contact.
	 */
	public ContactVO editContact(Map<String, ContactVO> obj,String editKey);

	/**
	 * Search a contact.
	 */
	public void searchContact(Map<String, ContactVO> obj, String searchKey);

}
