/*
 * This script is for swapping the landing page image between
 * one with black text (for light mode) and one with white
 * text (for dark mode).
 */

function swapLandingPageImage() {
    // Get the current theme (should be "slate" or "default")
    const colorSchemeValue = document.body.getAttribute("data-md-color-scheme");

    // Get the image element
    const imageElement = document.getElementById("landing-page-image");

    // Paths for light/dark mode images
    const lightModeImgPath = "assets/images/merlin_banner.png";
    const darkModeImgPath = "assets/images/merlin_banner_white.png";

    // Set the image source based on the color scheme
    imageElement.src = colorSchemeValue == "slate" ? darkModeImgPath : lightModeImgPath;
}

// Set up an observer to watch for theme changes
const observer = new MutationObserver(swapLandingPageImage);
const targetNode = document.body;
const config = {attributes: true, childList: true};
observer.observe(targetNode, config);