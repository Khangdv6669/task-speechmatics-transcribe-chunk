const gulp = require('gulp'),
	gulpEslint = require('gulp-eslint'),
	gulpDebug = require('gulp-debug'),
	gulpJasmine = require('gulp-jasmine'),
	istanbul = require('gulp-istanbul'),
	coveralls = require('gulp-coveralls');

var allOfMyFiles = [
	'!node_modules/**',
	'!**/node_modules/**',
    '!coverage/**',
    '!Gulpfile.js',
    // '*.js',
	'./**/*.js',
    '!**/*.spec.js'
];

var allOfMyTestFiles = [
	'!node_modules/**',
	'!**/node_modules/**',
    '!coverage/**',
    '!Gulpfile.js',
	'**/*.spec.js'
];

gulp.task('pre-test', function () {
    return gulp.src(allOfMyFiles)
    // Covering files
        .pipe(istanbul({includeUntested: true}))
        // Force `require` to return covered files
        .pipe(istanbul.hookRequire());
});

gulp.task('lint', function lint() {
	return gulp.src(allOfMyFiles.concat(allOfMyTestFiles))
        .pipe(gulpDebug({ title: 'lint:' }))
        .pipe(gulpEslint())
        .pipe(gulpEslint.format())
        .pipe(gulpEslint.failAfterError());
});

gulp.task('jasmine', function jasmine() {
	return gulp.src(allOfMyTestFiles)
        .pipe(gulpDebug({ title: 'jasmine:' }))
        .pipe(gulpJasmine({ verbose: true }));
});

gulp.task('coverage', ['pre-test', 'jasmine'], function jasmine() {
    return gulp.src(allOfMyTestFiles)
        .pipe(istanbul.writeReports())
        // Enforce a coverage of at least 90%
        .pipe(istanbul.enforceThresholds({ thresholds: { global: 0 } }));
});

gulp.task('coveralls', ['coverage'], function jasmine() {
    if (!process.env.COVERALLS_REPO_TOKEN) return;
	return gulp.src('coverage/**/lcov.info')
        .pipe(coveralls());
});

// This is a blues riff in "B", watch me for the changes, and try and keep up? - Marty McFly
// Rerun the task when a file changes
gulp.task('watch', function watch() {
	console.log('Please hold. This takes a few moments to start...');
	return gulp.watch(allOfMyFiles.concat(allOfMyTestFiles), ['test']);
});

gulp.task('test', ['lint', 'jasmine', 'coverage', 'coveralls']);

gulp.task('default', ['test']);
