<?php
namespace elasticsearch;

class Util {
	/**
	 * Checks if passed array is associative.
	 *
	 * @param array $array
	 *
	 * @return bool
	 */
	static function is_associative( $array ) {
		$keys = array_keys( $array );
		return array_keys( $keys ) !== $keys;
	}
}
