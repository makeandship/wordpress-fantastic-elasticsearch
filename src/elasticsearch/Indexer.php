<?php
namespace elasticsearch;

/**
 * This class handles the magic of building documents and sending them to ElasticSearch for indexing.
 *
 * @license http://opensource.org/licenses/MIT
 * @author Paris Holley <mail@parisholley.com>
 * @version 4.0.1
 **/
class Indexer
{
	/**
	 * The number of posts to index per page when re-indexing
	 *
	 * @return integer posts per page
	 **/
	static function per_page()
	{
		return Config::apply_filters('indexer_per_page', 100);
	}

	/**
	 * Retrieve the posts for the page provided
	 *
	 * @param integer $page The page of results to retrieve for indexing
	 *
	 * @return WP_Post[] posts
	 **/
	static function get_posts($page = 1)
	{
		$args = Config::apply_filters('indexer_get_posts', array(
			'posts_per_page' => self::per_page(),
			'post_type' => Config::types(),
			'paged' => $page,
			'post_status' => 'publish'
		));

		return get_posts($args);
	}

	/**
	 * Retrieve count of the number of posts available for indexing
	 *
	 * @return integer number of posts
	 **/
	static function get_count()
	{
		$query = new \WP_Query(array(
			'post_type' => Config::types(),
			'post_status' => 'publish'
		));

		return $query->found_posts; //performance risk?
	}

	/**
	 * Removes all data in the ElasticSearch index
	 **/
	static function clear()
	{
		$indexName = Config::option('secondary_index') ?: Config::option('server_index');

		$index = self::_client(true)->getIndex($indexName);

		try {
			$index->delete();
		} catch (\Exception $ex) {
			// will throw an exception if index does not exist
		}

		$index->create(
			array(
				'number_of_shards' => Config::apply_filters('indexer_number_of_shards', 3),
				'number_of_replicas' => Config::apply_filters('indexer_number_of_replicas', 3)
			)
		);

		self::_map($index);
	}

	/**
	 * Re-index the posts on the given page in the ElasticSearch index
	 *
	 * @param integer $page The page to re-index
	 **/
	static function reindex($page = 1)
	{
		$indexName = Config::option('secondary_index') ?: Config::option('server_index');

		$index = self::_client(true)->getIndex($indexName);

		$posts = self::get_posts($page);

		$count = 0;

		// group bulk documents by post type
		$documents = array();
		foreach ($posts as $post) {
			$post_type = $post->post_type;

			$data = self::_build_document($post);
			$document = new \Elastica\Document($post->ID, $data);

			if (!array_key_exists($post_type, $documents)) {
				$documents[$post_type] = array();
			}

			$documents[$post_type][] = $document;

			$count++;
		}

		// bulk update per type
		foreach($documents as $post_type => $bulk) {
			error_log('Adding '.count($bulk).' to '.$post_type);
			$before = microtime(true);
			$index = ($index ?: self::_index(true));
			$type = $index->getType($post_type);

			$type->addDocuments($bulk);
			$type->getIndex()->refresh();	
			$after = microtime(true);
			$time = ($after-$before) . " sec";
			error_log($page.'. Indexed '.$post_type.' in '.$time);
		}
		
		error_log('Page '.$page.' indexed '.$count.' documents');

		return $count;
	}

	/**
	 * Removes a post from the ElasticSearch index
	 *
	 * @param WP_Post $post The wordpress post to remove
	 **/
	static function delete($post)
	{
		$index = self::_index(true);

		$type = $index->getType($post->post_type);

		try {
			$type->deleteById($post->ID);
		} catch (\Elastica\Exception\NotFoundException $ex) {
			// ignore
		}
	}

	/**
	 * Updates an existing document in the ElasticSearch index (or creates it if it doesn't exist)
	 *
	 * @param WP_Post $post The wordpress post to remove
	 **/
	static function addOrUpdate($post, $index = null)
	{
		$index = ($index ?: self::_index(true));

		$type = $index->getType($post->post_type);

		$data = self::_build_document($post);

		if ($data) {
			$type->addDocument(new \Elastica\Document($post->ID, $data));
		}
	}

	static function addBulk( $documents, $index = null) {

	}

	/**
	 * Reads F.E.S configuration and updates ElasticSearch field mapping information (this can corrupt existing data).
	 * @internal
	 **/
	static function _map($index = null)
	{
		$index = $index ?: self::_index(true);

		foreach (Config::types() as $postType) {
			$type = $index->getType($postType);

			$properties = array();

			self::_map_values($properties, $type, Config::taxonomies(), 'taxonomy');
			self::_map_values($properties, $type, Config::fields(), 'field');
			self::_map_values($properties, $type, Config::meta_fields(), 'meta');

			$properties = Config::apply_filters('indexer_map', $properties, $postType);

			$mapping = new \Elastica\Type\Mapping($type, $properties);
			$mapping->send();
		}
	}

	/**
	 * Takes a wordpress post object and converts it into an associative array that can be sent to ElasticSearch
	 *
	 * @param WP_Post $post wordpress post object
	 * @return array document data
	 * @internal
	 **/
	static function _build_document($post)
	{
		global $blog_id;
		$document = array('blog_id' => $blog_id);
		$document = self::_build_field_values($post, $document);
		$document = self::_build_dynamic_field_values($post, $document);
		$document = self::_build_meta_values($post, $document);
		$document = self::_build_tax_values($post, $document);
		return Config::apply_filters('indexer_build_document', $document, $post);
	}

	/**
	 * Add post meta values to elasticsearch object, only if they are present.
	 *
	 * @param WP_Post $post
	 * @param Array $document to write to es
	 * @return Array $document
	 * @internal
	 **/
	static function _build_dynamic_field_values($post, $document)
	{
		$link = get_permalink( $post->ID );
		$document['link'] = $link;

		return $document;
	}

	/**
	 * Add post meta values to elasticsearch object, only if they are present.
	 *
	 * @param WP_Post $post
	 * @param Array $document to write to es
	 * @return Array $document
	 * @internal
	 **/
	static function _build_meta_values($post, $document)
	{
		if( class_exists('acf') ) {
			$config = Config::meta_fields();

			$acf = get_fields($post->ID);
			$filtered = self::_filter_acf_meta_values( null, $acf, $config);
			$document = array_merge($document, $filtered);			
		}
		else {
			$keys = get_post_custom_keys($post->ID);

			if (is_array($keys)) {
				$meta_fields = self::_build_meta_values_match_keys( $keys, Config::meta_fields());

				foreach ($meta_fields as $field) {
					$val = get_post_meta($post->ID, $field, true);

					if (isset($val)) {
						$document[$field] = $val;
					}
				}
			}
		}

		return $document;
	}

	private static function _filter_acf_meta_values( $prefix, $acf, $configs) {
		$document = array();

		if (isset($acf) && $acf && is_array($acf)) {		
			foreach($acf as $acf_key => $acf_value) {
				$name = empty($prefix) ? $acf_key : $prefix.".".$acf_key;

				if (is_array( $acf_value )) {
					if (Util::is_associative($acf_value)) {
						$filtered = self::_filter_acf_meta_values( $name, $acf_value, $configs);
						if (!empty($filtered)) {
							$document[$acf_key] = $filtered;
						}
					}
					else {
						$matches = array();
						foreach($acf_value as $index => $acf_value_item) {
							// wrap text field in array to match method signature
							$item = is_array($acf_value_item) ? $acf_value_item : [$acf_value_item]; 
							$filtered = self::_filter_acf_meta_values( $name, $item, $configs);
							
							if (!empty($filtered)) {
								$matches[] = $filtered;
							}
						}
						if (!empty($matches)) {
							$document[$acf_key] = $matches;
						}
					} 
				}
				else {
					if (in_array($name, $configs) && !empty($acf_value)) {
						$document[$acf_key] = $acf_value;
					}
				}
			}
		}

		return $document;
	}

	/**
	 * Match custom fields provided by the elastic field configuration with
	 * fields in a post
	 * 
	 */
	private static function _build_meta_values_match_keys( $keys, $configs ) {
		$matches = array();

		if (isset($keys)) {
			foreach($keys as $key) {
				foreach($configs as $config) {
					if (strrpos($config, ".") > -1) {
						$pattern = "/^".str_replace( ".", "_[0-9]+_", $config)."/";
						
						if (preg_match( $pattern, $key)) {
							$matches[] = $key;
						}
					}
					else if ($key === $config) {
						$matches[] = $key;
					}
				}
			}
		}

		return $matches;
	}

	/**
	 * Add post fields to new elasticsearch object, if the field is set
	 *
	 * @param WP_Post $post
	 * @param Array $document to write to es
	 * @return Array $document
	 * @internal
	 **/
	static function _build_field_values($post, $document)
	{
		foreach (Config::fields() as $field) {
			if (isset($post->$field)) {
				if ($field == 'post_date') {
					$document[$field] = date('c', strtotime($post->$field));
				} else if ($field == 'post_content') {
					$document[$field] = strip_tags($post->$field);
				} else {
					$document[$field] = $post->$field;
				}
			}
		}
		return $document;
	}

	/**
	 * Add post taxonomies to elasticsearch object
	 *
	 * @param WP_Post $post
	 * @param Array $document to write to es
	 * @return Array $document
	 * @internal
	 **/
	static function _build_tax_values($post, $document)
	{

		if (!isset($post->post_type))
			return $document;

		$taxes = array_intersect(Config::taxonomies(), get_object_taxonomies($post->post_type));
		foreach ($taxes as $tax) {
			$document[$tax] = array();

			foreach (wp_get_object_terms($post->ID, $tax) as $term) {
				if (!in_array($term->slug, $document[$tax])) {
					$document[$tax][] = $term->slug;
					$document[$tax . '_name'][] = $term->name;
				}

				$doParents = Config::apply_filters('indexer_tax_values_parent', true, $tax);

				if (isset($term->parent) && $term->parent && $doParents) {
					$parent = get_term($term->parent, $tax);

					while ($parent != null) {
						if (!in_array($parent->slug, $document[$tax])) {
							$document[$tax][] = $parent->slug;
							$document[$tax . '_name'][] = $parent->name;
						}

						if (isset($parent->parent) && $parent->parent) {
							$parent = get_term($parent->parent, $tax);
						} else {
							$parent = null;
						}
					}
				}
			}
		}
		return $document;
	}

	/**
	 * Create new ES field mappings for the given fields from the configuration
	 * TODO align callback names with Config::names so we can simply call this method with the kind string
	 * @param array $config_fields
	 * @param string $kind of internal fields: meta|field|taxonomy used to call the right indexer_map filter
	 */
	static function _map_values(&$properties, $type, $config_fields, $kind)
	{
		$index = self::_index(false);
		$numeric = Config::option('numeric');
		$notanalyzed = Config::option('not_analyzed');

		foreach ($config_fields as $field) {
			$base = $field;
			$pos = strpos($field, '.');
			if ($pos !== false) {
				// e.g. urls.url_title
				$base = substr($field, 0, $pos); // e.g. urls
				$remainder = substr($field, $pos + 1, strlen($field) - $pos); // e.g. url_title
				
				$props = array_key_exists($base, $properties) ? $properties[$base] : array('type' => 'nested');
				$nested_properties = array_key_exists('properties', $props) ? $props['properties'] : array();
				self::_map_values($nested_properties, $type, [$remainder], $kind);
				$props['properties'] = $nested_properties;
			}
			else {
				// set default
				$props = array('type' => 'string');
				// detect special field type
				if (isset($numeric[$field])) {
					$props['type'] = 'float';
				} elseif (isset($notanalyzed[$field]) || $kind == 'taxonomy' || $field == 'post_type') {
					$props['index'] = 'not_analyzed';
				} elseif ($field == 'post_date') {
					$props['type'] = 'date';
					$props['format'] = 'date_time_no_millis';
				} else {
					$props['index'] = 'analyzed';
				}

				if ($props['type'] == 'string' && $props['index'] == 'analyzed') {
					// provides more accurate searches

					$lang = Config::apply_filters('string_language', 'english');
					$props = array(
						'type' => 'multi_field',
						'fields' => array(
							$field => $props,
							$lang => array_merge($props, array(
								'analyzer' => $lang
							))
						)
					);
				}
			}


			// generic filter indexer_map_field| indexer_map_meta | indexer_map_taxonomy
			$props = Config::apply_filters('indexer_map_' . $kind, $props, $field);

			// also index taxonomy_name field
			if ($kind == 'taxonomy') {
				$tax_name_props = array('type' => 'string');
				$tax_name_props = Config::apply_filters('indexer_map_taxonomy_name', $tax_name_props, $field);
			}

			$properties[$base] = $props;

			if (isset($tax_name_props)) {
				$properties[$base . '_name'] = $tax_name_props;
			}
		}
	}

	/**
	 * The Elastica\Client object used by F.E.S
	 *
	 * @param boolean $write Specifiy whether you are making read-only or write transactions (currently just adjusts timeout values)
	 *
	 * @return Elastica\Client
	 * @internal
	 **/
	static function _client($write = false)
	{
		$settings = array(
			'url' => Config::option('server_url')
		);

		if ($write) {
			$settings['timeout'] = Config::option('server_timeout_write') ?: 300;
		} else {
			$settings['timeout'] = Config::option('server_timeout_read') ?: 1;
		}

		// Allow custom settings to be passed by users who want to.
		$settings = apply_filters('indexer_client_settings', $settings);

		return new \Elastica\Client($settings);
	}

	/**
	 * The Elastica\Index object used by F.E.S
	 *
	 * @param boolean $write Specifiy whether you are making read-only or write transactions (currently just adjusts timeout values)
	 *
	 * @return Elastica\Index
	 * @internal
	 **/
	static function _index($write = false)
	{
		return self::_client($write)->getIndex(Config::option('server_index'));
	}
}

?>
